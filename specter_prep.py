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
def parse_metadata_shard(data_dir, shard_num, output_citation_data, fields=None):
    
    metadata_file = gzip.open(
        os.path.join(data_dir, 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')
    
    reader = jsonlines.Reader(metadata_file)

    print("Metadata shard {} start".format(shard_num))
    
    for paper in tqdm.tqdm(reader.iter(skip_invalid=True)):
        # Only consider papers that have outbound citations available
        if not paper['has_outbound_citations'] or not paper['mag_field_of_study']:
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

def add_indirect_citations(citation_data_direct, shard_num, citation_data_indirect):
    
    other_shard_nums = list(range(len(citation_data_direct.keys())))
    other_shard_nums.remove(shard_num)

    tqdm_text = "#" + "{}".format(shard_num).zfill(3)
    
    pbar = tqdm.tqdm(
        total=len(citation_data_direct[shard_num].keys()), desc=tqdm_text, position=shard_num+1)

    for paper_id in citation_data_direct[shard_num].keys():
        directly_cited_ids = citation_data[paper_id].keys()
        
        # Search each shards
        for n in other_shard_nums:
            indirect_citations = get_citations_by_ids(citation_data_direct, n, directly_cited_ids)

            for indirect_id in indirect_citations:
                # This indirect citation would serve as a hard negative only if the paper_id
                # doesn't cite it in the first place.
                if indirect_id not in directly_cited_ids:
                    citation_data_indirect[paper_id][indirect_id] = {"count": 1} # 1 = "a citation of a citation"
                    
def get_citations_by_ids(citation_data_direct, shard_num, directly_cited_ids):
    
    citations = set()
    
    citation_data = manager_dict[shard_num]
        
    matching_ids = set(ids).intersection(set(citation_data.keys()))

    for paper_id in matching_ids:
        citations.union(set(citation_data[paper_id].keys()))
        
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
    citation_data_indirect = manager.dict()
    
    # Parse `metadata` from s2orc to create `data.json` for SPECTER
    metadata_read_pool = multiprocessing.Pool(processes=args.num_processes)
    metadata_read_results = []
    
    for i in range(shards_total_num):
        metadata_read_results.append(
            metadata_shard_pool.apply_async(
                parse_metadata_shard, 
                args=(os.path.join(args.data_dir, 'metadata'), i, citation_data_direct, args.fields_of_study)))

    metadata_read_pool.close()
    metadata_read_pool.join()
    
    # Scan intermediate data_{}.json files (currently with direct citation only)
    # for indirect citations
    indirect_citations_pool = multiprocessing.Pool(processes=args.num_processes)
    indirect_citations_results = []
    
    for i in range(shards_total_num):
        indirect_citations_results.append(
            indirect_citations_pool.apply_async(add_indirect_citations, args=(citation_data_direct, i, citation_data_indirect)))

    indirect_citations_pool.close()
    indirect_citations_pool.join()
    
    # Combine citation_data_direct and citation_data_indirect into a single json file.
    citation_data_all = {}
    
    for k in citation_data_direct.keys():
        citation_data_all[k] = {**citation_data_direct[k], **citation_data_indirect[k]}
        
    # Write citation_data_all to a file.
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "data.json"), 'w+')
    
    json.dump(output_citation_data, output_file, indent=2)
    
    output_file.close()
