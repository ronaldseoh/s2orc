import os
import pathlib
import multiprocessing
import argparse
import gzip
import json

import jsonlines
import tqdm


def parse_pdf_parses_shard(data_dir, shard_num, save_dir):

    pdf_parses_file = gzip.open(
        os.path.join(data_dir, 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')

    reader = jsonlines.Reader(pdf_parses_file)

    pass

# Process metadata jsonl into `data.json` as required by SPECTER.
# Need to get all the citation information.
def parse_metadata_shard(data_dir, shard_num, fields=None):

    output_citation_data = {}

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

        # Iterate through paper ids of outbound citations
        citations = {}

        for out_id in paper['outbound_citations']:
            citations[out_id] = {"count": 5} # 5 = direct citation

        output_citation_data[paper['paper_id']] = citations

        pbar.update(1)

    return output_citation_data

def get_indirect_citations(citation_data_direct, ids):

    citation_data_indirect = {}

    print("Finding indirect citations for {} papers".format(len(ids)))

    pbar = tqdm.tqdm(
        total=len(ids),
        desc="#" + "{}".format(os.getpid()).zfill(6))

    for paper_id in ids:
        directly_cited_ids = citation_data_direct[paper_id].keys()

        citation_data_indirect[paper_id] = {}

        # Search each shards
        indirect_citations = get_citations_by_ids(citation_data_direct, directly_cited_ids)

        for indirect_id in indirect_citations:
            # This indirect citation would serve as a hard negative only if the paper_id
            # doesn't cite it in the first place.
            if indirect_id not in directly_cited_ids:
                citation_data_indirect[paper_id][indirect_id] = {"count": 1} # 1 = "a citation of a citation"

        pbar.update(1)

    return citation_data_indirect

def get_citations_by_ids(citation_data_direct, ids):

    citations = set()

    for paper_id in ids:
        citations.union(set(citation_data_direct[paper_id].keys()))

    return citations


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--fields_of_study', nargs='*', type=str)
    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Total number of shards to process
    shards_total_num = 100

    manager = multiprocessing.Manager()

    citation_data_direct = manager.dict()

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

    print("Saving the parsed metadata to a manager dict...")
    metadata_shard_paper_ids = []
    
    for r in tqdm.tqdm(metadata_read_results):
        rs = r.get()
        
        metadata_shard_paper_ids.append(rs.keys())

        citation_data_direct.update(rs)

    # Scan intermediate data_{}.json files (currently with direct citation only)
    # for indirect citations
    indirect_citations_pool = multiprocessing.Pool(processes=args.num_processes)
    indirect_citations_results = []

    for i in range(shards_total_num):
        indirect_citations_results.append(
            indirect_citations_pool.apply_async(get_indirect_citations, args=(citation_data_direct, metadata_shard_paper_ids[i])))

    indirect_citations_pool.close()
    indirect_citations_pool.join()

    # Combine citation_data_direct and citation_data_indirect into a single json file.
    print("Merging direct and indirect citations...")

    citation_data_all = {}

    for i in range(shards_total_num):
        direct = metadata_read_results[i].get()
        indirect = indirect_citations_results[i].get()

        assert len(direct.keys()) == len(indirect.keys())

        for paper_id in direct.keys():
            citation_data_all[paper_id] = {**direct[paper_id], **indirect[paper_id]}

    # Write citation_data_all to a file.
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "data.json"), 'w+')

    json.dump(output_citation_data, output_file, indent=2)

    output_file.close()
