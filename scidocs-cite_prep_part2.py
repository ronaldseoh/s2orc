import os
import pathlib
import multiprocessing
import argparse
import gzip
import warnings

import ujson as json
import tqdm


def parse_pdf_parses_shard(shard_num):

    output_metadata = {}

    pbar = tqdm.tqdm(position=shard_num+1)

    pdf_parses_file = gzip.open(
        os.path.join(args.data_dir, 'pdf_parses', 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')

    for line in pdf_parses_file:
        paper = json.loads(line)

        try:
            if all_paper_ids_by_shard[shard_num][paper['paper_id']]:
                output_metadata[paper['paper_id']] = {
                    'paper_id': paper['paper_id'],
                    'title': titles[paper['paper_id']],
                    'abstract': paper['abstract'][0]['text'],
                }
        except:
            pbar.update(1)
            continue

        pbar.update(1)

    pdf_parses_file.close()

    return output_metadata


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_json', help='path to data.json.')
    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')
    parser.add_argument('safe_paper_ids_json', help='path to safe_paper_ids.json.')
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

    # Load safe_paper_ids.json
    print("Loading safe_paper_ids.json...")
    safe_paper_ids = json.load(open(args.safe_paper_ids_json, 'r'))

    # Read titles.json and get all the titles
    print("Loading titles.json...")
    titles = json.load(open(args.titles_json, 'r'))
    
    print("Grouping all paper ids again by shard...")
    all_paper_ids_by_shard = []
    
    for i in range(SHARDS_TOTAL_NUM):
        all_paper_ids_by_shard.append({})
    
    for p_id in tqdm.tqdm(all_paper_ids):
        # Check which shard this paper belongs to by checking safe_paper_ids.
        # Note that this step is necessary as safe_paper_ids also contain papers
        # that are not part of data.json.
        shard_num = safe_paper_ids[p_id]
        
        if shard_num > -1:
            all_paper_ids_by_shard[shard_num][p_id] = True
        else:
            warnings.warn("Actually, there shouldn't be any papers without valid shard_num at this point. Make sure that your copy of specter_prep_data.py is working correctly. paper_id = " + str(p_id))

    # Parse `pdf_parses` from s2orc to create `metadata.json` for SPECTER
    print("Parsing pdf_parses...")
    pdf_parses_read_pool = multiprocessing.Pool(processes=args.num_processes)
    pdf_parses_read_results = []

    for i in range(SHARDS_TOTAL_NUM):
        pdf_parses_read_results.append(
            pdf_parses_read_pool.apply_async(
                parse_pdf_parses_shard, args=(i,)
            )
        )

    pdf_parses_read_pool.close()
    pdf_parses_read_pool.join()

    print("Combining all title/abstract from the shards...")
    metadata = json.load(open(args.data_json, 'r'))

    for r in tqdm.tqdm(pdf_parses_read_results):
        result = r.get()

        for p_id in result.keys():
            metadata[p_id].update(result[p_id])

    # All papers in all_paper_ids must not have their metadata included un `metadata`
    assert len(metadata.keys()) == len(all_paper_ids)

    # Write metadata to a file.
    print("Writing the metadata to data_final.json...")
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    output_file = open(os.path.join(args.save_dir, "data_final.json"), 'w+')

    json.dump(metadata, output_file)

    output_file.close()
