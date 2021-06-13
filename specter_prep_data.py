import os
import pathlib
import multiprocessing
import argparse
import gzip
import random

import ujson as json
import jsonlines
import tqdm

global citation_data_direct
citation_data_direct = {}


# Process metadata jsonl into `data.json` as required by SPECTER.
# Need to get all the citation information.
def parse_metadata_shard(data_dir, shard_num, fields=None):

    output_citation_data = {}
    output_paper_ids_by_field = {}

    metadata_file = gzip.open(
        os.path.join(data_dir, 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')

    reader = jsonlines.Reader(metadata_file)

    print("Reading metadata shard {}".format(shard_num))

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(6),
        position=shard_num+1)

    for paper in reader.iter(skip_invalid=True):
        # Only consider papers that
        # have outbound citations available, and
        # have MAG field of study specified, and
        # PDF parse is available & abstract is included in PDF parse
        if not paper['has_outbound_citations'] \
           or not paper['mag_field_of_study'] \
           or not paper['has_pdf_parse']:
            continue
        elif not paper['has_pdf_parsed_abstract']:
            continue

        # if args.fields_of_study is specified, only consider the papers from
        # those fields
        if fields and not set(fields).isdisjoint(set(paper['mag_field_of_study'])):
            continue

        if paper['paper_id'] in output_citation_data.keys():
            print("Metadata shard {} Duplicate paper id {} found. Please check.".format(shard_num, paper['paper_id']))
        else:
            # Record paper_id based on mag_field_of_study
            for paper_field in paper['mag_field_of_study']:
                if paper_field not in output_paper_ids_by_field.keys():
                    output_paper_ids_by_field[paper_field] = []
                    
                output_paper_ids_by_field[paper_field].append(paper['paper_id'])

            # Iterate through paper ids of outbound citations
            citations = {}

            for out_id in paper['outbound_citations']:
                citations[out_id] = {"count": 5} # 5 = direct citation

            output_citation_data[paper['paper_id']] = citations

        pbar.update(1)

    return output_citation_data, output_paper_ids_by_field

def get_indirect_citations(ids):

    citation_data_indirect = {}

    for paper_id in ids:
        directly_cited_ids = citation_data_direct[paper_id].keys()

        citation_data_indirect[paper_id] = {}

        # Search each shards
        indirect_citations = get_citations_by_ids(directly_cited_ids)

        for indirect_id in indirect_citations:
            # This indirect citation would serve as a hard negative only if the paper_id
            # doesn't cite it in the first place.
            if indirect_id not in directly_cited_ids:
                citation_data_indirect[paper_id][indirect_id] = {"count": 1} # 1 = "a citation of a citation"

    return citation_data_indirect

def get_citations_by_ids(ids):

    citations = set()

    for paper_id in ids:
        try:
            citations = citations.union(set(citation_data_direct[paper_id].keys()))
        except:
            continue

    return citations
    
def get_all_paper_ids(data):
    
    all_paper_ids = set()
    
    for paper_id in data.keys():
        all_paper_ids.add(paper_id)
        
        all_paper_ids = all_paper_ids.union(set(data[paper_id].keys()))
    
    return list(all_paper_ids)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--fields_of_study', nargs='*', type=str)
    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')
    
    parser.add_argument('--seed', default=321, type=int, help='Random seed.')
    
    parser.add_argument('--query_shard', type=int, help='the shard to be used for extracting query papers.')
    parser.add_argument('--validation_shard', type=int, help='the shard to be used for extracting validation papers.')

    args = parser.parse_args()

    # Random seed fix for Python random
    # Will be used for train/val splitting
    random.seed(args.seed)

    # Total number of shards to process
    shards_total_num = 100
    
    # Check query/validation shard
    if args.query_shard:
        if not (args.query_shard >= 0 and args.query_shard < shard_total_num):
            raise Exception("Invalid value for args.query_shard: {}".format(args.query_shard))
        else:
            query_shard = args.query_shard
            
    if args.validation_shard:
        if not (args.validation_shard >= 0 and args.validation_shard < shard_total_num):
            raise Exception("Invalid value for args.validation_shard: {}".format(args.validation_shard))
        else:
            validation_shard = args.validation_shard

    # Parse `metadata` from s2orc to create `data.json` for SPECTER
    metadata_read_pool = multiprocessing.Pool(processes=args.num_processes)
    metadata_read_results = []

    for i in range(shards_total_num):
        metadata_read_results.append(
            metadata_read_pool.apply_async(
                parse_metadata_shard,
                args=(os.path.join(args.data_dir, 'metadata'), i, args.fields_of_study)))

    metadata_read_pool.close()
    metadata_read_pool.join()

    print("Saving the parsed metadata to a single dict...")
    
    for r in tqdm.tqdm(metadata_read_results):

        citation_data_by_shard, paper_ids_by_field = r.get()

        citation_data_direct.update(citation_data_by_shard)

    print("Adding indirect citations...")

    # Scan intermediate data_{}.json files (currently with direct citation only)
    # for indirect citations
    indirect_citations_pool = multiprocessing.Pool(processes=args.num_processes)
    indirect_citations_results = []
    
    indirect_citations_imap_iterator = indirect_citations_pool.imap_unordered(
        get_indirect_citations, [(paper_id,) for paper_id in citation_data_direct.keys()], chunksize=1000)

    for r in tqdm.tqdm(indirect_citations_imap_iterator):
        indirect_citations_results.append(r)

    # Combine citation_data_direct and citation_data_indirect into a single json file.
    print("Merging direct and indirect citations...")

    citation_data_all = citation_data_direct

    for r in indirect_citations_results:
        indirect = r.get()

        for paper_id in indirect.keys():
            citation_data_all[paper_id].update(indirect[paper_id])

    # Write citation_data_all to a file.
    print("Writing data.json to a file.")

    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "data.json"), 'w+')

    json.dump(citation_data_all, output_file, indent=2)

    output_file.close()

    # Get all paper ids and dump them to a file as well.
    print("Getting all paper ids ever appearing in data.json.")
    all_paper_ids = get_all_paper_ids(citation_data_all)
    
    print("Writing all paper ids to a file.")
    all_paper_ids_output_file = open(os.path.join(args.save_dir, "paper_ids.json"), 'w+')

    json.dump(all_paper_ids, all_paper_ids_output_file, indent=2)

    all_paper_ids_output_file.close()
