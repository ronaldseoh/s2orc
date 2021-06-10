import os
import pathlib
import multiprocessing
import argparse
import gzip
import json
import copy

import jsonlines
import tqdm


def parse_pdf_parses_shard(data_dir, shard_num, save_dir):

    pdf_parses_file = gzip.open(
        os.path.join(data_dir, 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')
    
    reader = jsonlines.Reader(pdf_parses_file)
    
    pass

# Process metadata jsonl into `data.json` as required by SPECTER.
# Need to get all the citation information.
def parse_metadata_shard(data_dir, shard_num, save_dir, fields=None):
    
    output_citation_data = {}
    
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

    # Save to a json file
    output_file = open(
        os.path.join(save_dir, "data_{}.json".format(shard_num)), 'w+')
    
    json.dump(output_citation_data, output_file, indent=2)
    
    output_file.close()

def read_json(manager_dict, key, json_path):
    
    manager_dict[key] = json.load(open(json_path, 'r'))

    print("Loaded", json_path, "into memory.")

def add_indirect_citations(manager_dict, shard_num):
    
    other_shard_nums = list(range(len(manager_dict.keys()))
    other_shard_nums.remove(shard_num)
    
    citation_data = manager_dict[shard_num]
    
    output_citation_data = copy.deepcopy(citation_data)
    
    print("Adding indirect citations shard {}".format(shard_num))
    
    for paper_id in tqdm.tqdm(citation_data.keys()):
        direct_citations = citation_data[paper_id].keys()

        search_results = []
        
        # Search each other shards simultaneously
        for n in other_shard_nums:
            search_results.append(
                get_citations_by_ids(manager_dict, n, direct_citations))

        # Add indirect citations to citation_data
        for citations in search_results:
            for indirect_id in citations:
                output_citation_data[paper_id][indirect_id] = {"count": 1} # 1 = "a citation of a citation"

    # Save the modified citation_data to a file
    output_file = open(
        os.path.join(temp_dir, "data_{}_with_indirect.json".format(shard_num)), 'w+')
    
    json.dump(output_citation_data, output_file, indent=2)
    
    output_file.close()
                    
def get_citations_by_ids(manager_dict, shard_num, ids):
    
    citations = set()
    
    citation_data = manager_dict[shard_num]
        
    matching_ids = set(ids).intersection(set(citation_data).keys())

    for paper_id in matching_ids:
        citations.union(set(citation_data[paper_id].keys()))
        
    return citations


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--fields_of_study', nargs='*', type=str)
    
    args = parser.parse_args()

    # create a temp dir
    pathlib.Path('temp').mkdir(exist_ok=True)
    
    # Parse `metadata` from s2orc to create `data.json` for SPECTER
    metadata_shard_pool = multiprocessing.Pool(processes=10)
    
    # Total number of shards to process
    shards_total_num = 100
    
    for i in range(shards_total_num):
        p = metadata_shard_pool.apply_async(
            parse_metadata_shard, 
            args=(os.path.join(args.data_dir, 'metadata'), i, 'temp', args.fields_of_study))

    metadata_shard_pool.close()
    metadata_shard_pool.join()
    
    # Load all data_{}.json into memory for further processing.
    with multiprocessing.Manager() as manager:
        temp_data = manager.dict()
        temp_data_loading_pool = multiprocessing.Pool(processes=10)
        
        for i in range(shards_total_num):
            p = temp_data_loading_pool.apply_async(
                read_json,
                args=(temp_data, i, os.path.join('temp', 'data_{}.json'.format(i))))

        temp_data_loading_pool.close()
        temp_data_loading_pool.join()
        
        # Scan intermediate data_{}.json files (currently with direct citation only)
        # for indirect citations
        
        indirect_citations_pool = multiprocessing.Pool(processes=10)
        
        for i in range(shards_total_num):
            p = indirect_citations_pool.apply_async(add_indirect_citations, args=(temp_data, i))

        indirect_citations_pool.close()
        indirect_citations_pool.join()
