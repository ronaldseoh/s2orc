import os
import pathlib
import multiprocessing
import argparse
import gzip
import json

import jsonlines
import tqdm


def get_all_paper_ids(data):
    
    all_paper_ids = set()
    
    for paper_id in data.keys():
        all_paper_ids.add(paper_id)
        
        all_paper_ids = all_paper_ids.union(set(data[paper_id].keys()))
    
    return all_paper_ids

def parse_pdf_parses_shard(data_dir, shard_num, ids):
    
    output_metadata = {}

    pdf_parses_file = gzip.open(
        os.path.join(data_dir, 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')

    reader = jsonlines.Reader(pdf_parses_file)

    pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_json', help='path to data.json.')
    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Total number of shards to process
    shards_total_num = 100

    # Parse `pdf_parses` from s2orc to create `metadata.json` for SPECTER
    pdf_parses_read_pool = multiprocessing.Pool(processes=args.num_processes)
    pdf_parses_read_results = []

    for i in range(shards_total_num):
        metadata_read_results.append(
            metadata_read_pool.apply_async(
                parse_metadata_shard,
                args=(os.path.join(args.data_dir, 'metadata'), i, args.fields_of_study)))

    metadata_read_pool.close()
    metadata_read_pool.join()

    # Write citation_data_all to a file.
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "data.json"), 'w+')

    json.dump(output_citation_data, output_file, indent=2)

    output_file.close()
