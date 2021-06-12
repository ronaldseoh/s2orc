import os
import pathlib
import multiprocessing
import argparse
import gzip
import json

import jsonlines
import tqdm


# Process metadata jsonl and get all the paper titles.
def parse_metadata_shard(data_dir, shard_num, ids):

    output_title = {}

    metadata_file = gzip.open(
        os.path.join(data_dir, 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')

    reader = jsonlines.Reader(metadata_file)

    print("Reading metadata shard {}".format(shard_num))

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(6),
        position=shard_num+1)

    for paper in reader.iter(skip_invalid=True):

        if paper['paper_id'] in ids:
            output_title[paper['paper_id']] = paper['title']

        pbar.update(1)

    return output_title


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Total number of shards to process
    shards_total_num = 100
    
    # Load paper_ids.json
    all_paper_ids = json.load(open(args.paper_ids_json, 'r'))

    # Parse `metadata` from s2orc to get all the paper titles
    metadata_read_pool = multiprocessing.Pool(processes=args.num_processes)
    metadata_read_results = []

    for i in range(shards_total_num):
        metadata_read_results.append(
            metadata_read_pool.apply_async(
                parse_metadata_shard,
                args=(os.path.join(args.data_dir, 'metadata'), i, all_paper_ids)))

    metadata_read_pool.close()
    metadata_read_pool.join()

    print("Saving the parsed metadata to a dictionary...")
    
    paper_titles = {}
    
    for r in tqdm.tqdm(metadata_read_results):

        rs = r.get()

        for paper_id in rs.keys():
            paper_titles[paper_id] = rs[paper_id]

    # Write citation_data_all to a file.
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "titles.json"), 'w+')

    json.dump(paper_titles, output_file, indent=2)

    output_file.close()
