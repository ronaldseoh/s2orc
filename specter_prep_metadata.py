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

def parse_pdf_parses_shard(data_dir, shard_num, titles, ids):
    
    output_metadata = {}

    pdf_parses_file = gzip.open(
        os.path.join(data_dir, 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')

    reader = jsonlines.Reader(pdf_parses_file)

    for paper in reader.iter(skip_invalid=True):
        
        if paper['paper_id'] in ids:
            output_metadata[paper['paper_id']] = {
                'title': titles[paper['paper_id']],
                'abstract': paper['abstract'][0]['text'],
            }
            
    return output_metadata


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_json', help='path to data.json.')
    parser.add_argument('titles_json', help='path to titles.json.')

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Total number of shards to process
    shards_total_num = 100
    
    # Read data.json and get all the paper ids
    data = json.load(open(args.data_json, 'r'))
    
    all_paper_ids = get_all_paper_ids(data)
    
    # Read titles.json and get all the titles
    titles = json.load(open(args.titles_json, 'r'))

    # Parse `pdf_parses` from s2orc to create `metadata.json` for SPECTER
    pdf_parses_read_pool = multiprocessing.Pool(processes=args.num_processes)
    pdf_parses_read_results = []

    for i in range(shards_total_num):
        pdf_parses_read_results.append(
            pdf_parses_read_pool.apply_async(
                parse_pdf_parses_shard,
                args=(os.path.join(args.data_dir, 'pdf_parses'), i, titles, all_paper_ids)))

    pdf_parses_read_pool.close()
    pdf_parses_read_pool.join()
    
    metadata = {}
    
    for r in tqdm.tqdm(pdf_parses_read_results):

        metadata.update(r.get())

    # Write metadata to a file.
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "metadata.json"), 'w+')

    json.dump(metadata, output_file, indent=2)

    output_file.close()
