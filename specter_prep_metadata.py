import os
import pathlib
import multiprocessing
import argparse
import gzip

import ujson as json
import tqdm


def parse_pdf_parses_shard(paper_ids, tqdm_position):

    output_metadata = {}

    pbar = tqdm.tqdm(position=tqdm_position+1, total=len(paper_ids))

    for p_id in paper_ids:
        # Check which shard this paper belongs to by checking safe_paper_ids.
        shard_num = safe_paper_ids[p_id]

        pdf_parses_file = gzip.open(
            os.path.join(args.data_dir, 'pdf_parses', 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')

        for line in pdf_parses_file:
            paper = json.loads(line)

            if paper['paper_id'] == p_id:
                output_metadata[paper['paper_id']] = {
                    'title': titles[paper['paper_id']],
                    'abstract': paper['abstract'][0]['text'],
                }

                break

        pdf_parses_file.close()

        pbar.update(1)

    return output_metadata


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')
    parser.add_argument('safe_paper_ids_json', help='path to safe_paper_ids.json.')
    parser.add_argument('titles_json', help='path to titles.json.')

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Load paper_ids.json
    print("Loading paper_ids.json...")
    all_paper_ids = json.load(open(args.paper_ids_json, 'r'))

    # Load safe_paper_ids.json
    print("Loading safe_paper_ids.json...")
    safe_paper_ids = json.load(open(args.safe_paper_ids_json, 'r'))

    # Read titles.json and get all the titles
    print("Loading titles.json...")
    titles = json.load(open(args.titles_json, 'r'))

    # Parse `pdf_parses` from s2orc to create `metadata.json` for SPECTER
    print("Parsing pdf_parses...")
    pdf_parses_read_pool = multiprocessing.Pool(processes=args.num_processes)
    pdf_parses_read_results = []

    paper_ids_slice_size = len(all_paper_ids) // args.num_processes

    for i in range(args.num_processes + 1):

        ids_slice_start = i * paper_ids_slice_size
        ids_slice_end = (i+1) * paper_ids_slice_size

        pdf_parses_read_results.append(
            pdf_parses_read_pool.apply_async(
                parse_pdf_parses_shard,
                args=(all_paper_ids[ids_slice_start:ids_slice_end], i)
            )
        )

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
