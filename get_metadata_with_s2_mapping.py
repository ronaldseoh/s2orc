import os
import pathlib
import multiprocessing
import argparse
import gzip

import ujson as json
import tqdm


def parse_metadata_shard(shard_num):

    output_metadata = {}

    pbar = tqdm.tqdm(position=shard_num+1)

    metadata_file = gzip.open(
        os.path.join(args.data_dir, 'metadata', 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')

    for line in metadata_file:
        paper = json.loads(line)

        try:
            if str(paper['paper_id']) in mapping.keys():
                # make the key to be S2 id (used in the specter dataset)
                # instead of S2ORC id
                output_metadata[mapping[paper['paper_id']]] = {
                    's2orc_id': paper['paper_id'],
                    'inbound_citations_count': len(paper['inbound_citations']),
                }
        except:
            pbar.update(1)
            continue

        pbar.update(1)

    metadata_file.close()

    return output_metadata


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('s2id_to_s2orc_paper_id_json', help='path to s2id_to_s2orc_paper_id_json file.')

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()
    
    # Total number of shards to process
    SHARDS_TOTAL_NUM = 100

    # Load s2id_to_s2orc_paper_id json
    print("Loading s2id_to_s2orc_paper_id json...")
    mapping_original = json.load(open(args.s2id_to_s2orc_paper_id_json, 'r'))

    mapping = {v: k for k, v in mapping_original.items()}

    metadata_pool = multiprocessing.Pool(processes=args.num_processes)
    metadata_results = []

    for i in range(SHARDS_TOTAL_NUM):
        metadata_results.append(
            metadata_pool.apply_async(
                parse_metadata_shard, args=(i,)
            )
        )

    metadata_pool.close()
    metadata_pool.join()

    print("Combining all title/abstract from the shards...")
    metadata = {}

    for r in tqdm.tqdm(metadata_results):
        metadata.update(r.get())

    # Write metadata to a file.
    print("Writing the metadata to metadata.json...")
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "metadata.json"), 'w+')

    json.dump(metadata, output_file)

    output_file.close()
