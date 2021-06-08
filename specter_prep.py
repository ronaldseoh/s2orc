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

def add_indirect_citations(temp_dir, shard_num):
    
    other_shard_nums = list(range(100))
    other_shard_nums.remove(shard_num)
    
    citation_data = json.load(
        open(os.path.join(temp_dir, "data_{}.json".format(shard_num)), 'r').read())
    
    for paper_id in tqdm.tqdm(citation_data.keys()):
        direct_citations = citation_data[paper_id].keys()
        
        pool = multiprocessing.Pool()

        search_results = multiprocessing.Queue()
        
        # Search each other shards simultaneously
        for n in other_shard_nums:
            p = pool.apply_async(
                get_all_citations_by_ids,
                args=(temp_dir, n, direct_citations, search_results))

        pool.close()
        pool.join()
        
        # Add indirect citations to citation_data
        while not search_results.empty():
            citations = search_results.get()
            
            for indirect_id in citations:
                citations[indirect_id] = {"count": 1} # 1 = "a citation of a citation"

    # Save the modified citation_data to a file
    output_file = open(
        os.path.join(temp_dir, "data_{}_with_indirect.json".format(shard_num)), 'w+')
    
    json.dump(citation_data, output_file, indent=2)
    
    output_file.close()
                    
def get_all_citations_by_ids(temp_dir, shard_num, ids, results_queue):
    
    citations = set()
    
    citation_data = json.load(
        open(os.path.join(temp_dir, "data_{}.json".format(shard_num)), 'r').read())
        
    matching_ids = set(ids).intersection(set(citation_data).keys())
    
    for paper_id in matching_ids:
        if paper_id in citation_data.keys():
            citations.union(set(citation_data[paper_id].keys()))
        
    results_queue.put(citations)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--fields_of_study', nargs='*', type=str)
    
    args = parser.parse_args()

    # create a temp dir
    pathlib.Path('temp').mkdir(exist_ok=True)
    
    # Parse `metadata` from s2orc to create `data.json` for SPECTER
    metadata_shard_pool = multiprocessing.Pool()
    
    for i in range(100):
        p = metadata_shard_pool.apply_async(
            parse_metadata_shard, 
            args=(os.path.join(args.data_dir, 'metadata'), i, 'temp', args.fields_of_study))

    metadata_shard_pool.close()
    metadata_shard_pool.join()
    
    # Scan intermediate data_{}.json files (currently with direct citation only)
    # for indirect citations
    print("Adding indirect citations...")
    
    for i in range(100):
        print("Shard {}".format(i))
        add_indirect_citations('temp', i)
