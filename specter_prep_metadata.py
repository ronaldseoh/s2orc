import os
import pathlib
import multiprocessing
import argparse
import gzip

import ujson as json
import jsonlines
import tqdm


def parse_pdf_parses_shard(data_dir, shard_num):

    output_metadata = {}

    pdf_parses_file = gzip.open(
        os.path.join(data_dir, 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')

    reader = jsonlines.Reader(pdf_parses_file)

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3), position=shard_num+1)

    for paper_id in all_paper_ids:
        for paper in reader.iter(skip_invalid=True):
            if paper['paper_id'] == paper_id:
                output_metadata[paper['paper_id']] = {
                    'title': titles[paper['paper_id']],
                    'abstract': paper['abstract'][0]['text'],
                }

                break

        pbar.update(1)
        
    print("Shard {}: Found {} papers.".format(shard_num, len(output_metadata))

    return output_metadata


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')
    parser.add_argument('titles_json', help='path to titles.json.')

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Total number of shards to process
    SHARDS_TOTAL_NUM = 100

    # Load paper_ids.json
    print("Loading paper_ids.json...")
    all_paper_ids = json.load(open(args.paper_ids_json, 'r'))

    # Read titles.json and get all the titles
    print("Loading titles.json...")
    titles = json.load(open(args.titles_json, 'r'))

    # Parse `pdf_parses` from s2orc to create `metadata.json` for SPECTER
    print("Parsing pdf_parses...")
    pdf_parses_read_pool = multiprocessing.Pool(processes=args.num_processes)
    pdf_parses_read_results = []

    for i in range(SHARDS_TOTAL_NUM):
        pdf_parses_read_results.append(
            pdf_parses_read_pool.apply_async(
                parse_pdf_parses_shard,
                args=(os.path.join(args.data_dir, 'pdf_parses'), i)))

    pdf_parses_read_pool.close()
    pdf_parses_read_pool.join()

    print("Combining all title/abstract from the shards...")
    metadata = {}

    for r in tqdm.tqdm(pdf_parses_read_results):

        metadata.update(r.get())

    # Write metadata to a file.
    print("Writing the metadata to metadata.json...")
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "metadata.json"), 'w+')

    json.dump(metadata, output_file)

    output_file.close()
