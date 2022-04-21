import os
import pathlib
import multiprocessing
import argparse
import gzip
import random
import copy
import gc
import collections

import ujson as json
import tqdm


# Process metadata jsonl into `data.json` as required by SPECTER.
# Need to get all the citation information.
def parse_metadata_shard(shard_num, fields=None):

    output_citation_data = collections.defaultdict(dict)
    output_query_paper_ids = []
    output_query_paper_ids_by_field = collections.defaultdict(list)
    output_safe_paper_ids = {}
    output_titles = {}

    metadata_file = gzip.open(
        os.path.join(args.data_dir, 'metadata', 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')

    print("Reading metadata shard {}".format(shard_num))

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3),
        position=shard_num+1)

    for line in metadata_file:
        paper = json.loads(line)

        # Only consider papers that
        # have MAG field of study specified, and
        # PDF parse is available & abstract is included in PDF parse
        if not paper['mag_field_of_study'] \
           or not paper['has_pdf_parse']:
            output_safe_paper_ids[paper['paper_id']] = -1
            pbar.update(1)
            continue

        if not paper['has_pdf_parsed_abstract']:
            output_safe_paper_ids[paper['paper_id']] = -1
            pbar.update(1)
            continue

        # Since SPECTER requires all papers in the graph to have titles and abstract,
        # Once the conditions listed above has been met,
        # record the paper id in safe_paper_ids
        output_safe_paper_ids[paper['paper_id']] = shard_num

        output_titles[paper['paper_id']] = paper['title']

        # Query papers should have outbound citations
        if not paper['has_outbound_citations']:
            pbar.update(1)
            continue

        if args.cocite:
            if not paper['has_inbound_citations']:
                pbar.update(1)
                continue

        if paper['paper_id'] in output_citation_data.keys():
            print("Metadata shard {} Duplicate paper id {} found. Please check.".format(shard_num, paper['paper_id']))
        else:
            # if args.fields_of_study is specified, only consider the papers from
            # those fields
            if fields and not set(fields).isdisjoint(set(paper['mag_field_of_study'])):
                pbar.update(1)
                continue

            # Record paper_id
            output_query_paper_ids.append(paper['paper_id'])

            # Record paper_id based on mag_field_of_study
            for paper_field in paper['mag_field_of_study']:
                if fields and paper_field not in fields:
                    continue

                output_query_paper_ids_by_field[paper_field].append(paper['paper_id'])

            # Iterate through paper ids of outbound citations
            output_citation_data[paper['paper_id']]['cites'] = paper['outbound_citations']

            if args.cocite:
                output_citation_data[paper['paper_id']]['cited_by'] = paper['inbound_citations']

        pbar.update(1)

    metadata_file.close()

    return output_citation_data, output_query_paper_ids, output_query_paper_ids_by_field, output_safe_paper_ids, output_titles


def parse_metadata_get_mag_shard(shard_num):

    mag_fields_shard = {}

    pbar = tqdm.tqdm(position=shard_num+1)

    metadata_file = gzip.open(
        os.path.join(args.data_dir, 'metadata', 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')

    for line in metadata_file:
        paper = json.loads(line)

        try:
            if str(paper['paper_id']) in all_paper_ids:
                mag_fields_shard[paper['paper_id']] = paper["mag_field_of_study"]
        except:
            pbar.update(1)
            continue

        pbar.update(1)

    metadata_file.close()

    return mag_fields_shard


def sanitize_citation_data_direct(shard_num):

    # Remove all the "unsafe" papers from the shard's citation_data_direct,
    # while avoiding iterating again through all the metadata shards
    print("Removing invalid direct citations...")

    output_citation_data_direct = copy.deepcopy(citation_data_direct_by_shard[shard_num])
    output_query_paper_ids = copy.deepcopy(query_paper_ids_all_shard[shard_num])
    output_query_paper_ids_by_field = copy.deepcopy(query_paper_ids_by_field_all_shard[shard_num])

    # Outbound citations
    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3),
        total=len(citation_data_direct_by_shard[shard_num].keys()),
        position=shard_num+1)

    for paper_id in citation_data_direct_by_shard[shard_num].keys():
        for i, cited_id in enumerate(citation_data_direct_by_shard[shard_num][paper_id]["cites"]):
            if safe_paper_ids[cited_id] == -1:
                output_citation_data_direct[paper_id]["cites"].remove(cited_id)

        pbar.update(1)

    # Inbound citations
    if args.cocite:
        pbar = tqdm.tqdm(
            desc="#" + "{}".format(shard_num).zfill(3),
            total=len(citation_data_direct_by_shard[shard_num].keys()),
            position=shard_num+1)

        for paper_id in citation_data_direct_by_shard[shard_num].keys():
            for i, cited_id in enumerate(citation_data_direct_by_shard[shard_num][paper_id]["cited_by"]):
                if safe_paper_ids[cited_id] == -1:
                    output_citation_data_direct[paper_id]["cited_by"].remove(cited_id)

            pbar.update(1)

    print("Removing query ids that no longer have any citations.")
    query_ids_to_remove = []

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3),
        total=len(output_citation_data_direct.keys()),
        position=shard_num+1)

    for paper_id in output_citation_data_direct.keys():
        if len(output_citation_data_direct[paper_id]["cites"]) == 0:
            query_ids_to_remove.append(paper_id)
        elif args.cocite and len(output_citation_data_direct[paper_id]["cited_by"]) == 0:
            query_ids_to_remove.append(paper_id)

        pbar.update(1)

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3),
        total=len(query_ids_to_remove),
        position=shard_num+1)

    for id_to_delete in query_ids_to_remove:
        del output_citation_data_direct[id_to_delete]
        output_query_paper_ids.remove(id_to_delete)

        for field in output_query_paper_ids_by_field.keys():
            try:
                output_query_paper_ids_by_field[field].remove(id_to_delete)
            except:
                continue

        pbar.update(1)

    return output_citation_data_direct, output_query_paper_ids, output_query_paper_ids_by_field

def get_all_paper_ids(citation_data):

    all_ids = set()

    for paper_id in tqdm.tqdm(citation_data.keys()):
        all_ids.add(paper_id)

        for cited_id in citation_data[paper_id]['cites']:
            all_ids.add(cited_id)

        if args.cocite:
            for cited_id in citation_data[paper_id]['cited_by']:
                all_ids.add(cited_id)

    return list(all_ids)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--fields_of_study', nargs='*', type=str)
    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    parser.add_argument('--seed', default=321, type=int, help='Random seed.')

    parser.add_argument('--shards', nargs='*', type=int, help='Specific shards to be used.')

    parser.add_argument(
        '--val_proportion',
        default=0.4, type=float, help='proportion of the generated dataset to be reserved for validation.')

    parser.add_argument(
        '--test_proportion',
        default=0.4, type=float, help='proportion of the generated dataset to be reserved for test.')

    parser.add_argument(
        '--train_proportion',
        type=float, help='proportion of the generated dataset to be reserved for training.')

    parser.add_argument('--cocite', default=False, action='store_true')

    args = parser.parse_args()

    # Random seed fix for Python random
    # Will be used for train/val splitting
    random.seed(args.seed)

    # Total number of shards to process
    SHARDS_TOTAL_NUM = 100

    # Check query/validation shard
    if args.shards:
        for n in args.shards:
            if not (n >= 0 and n < SHARDS_TOTAL_NUM):
                raise Exception("Invalid value for args.query_shard: {}".format(n))

    # Parse `metadata` from s2orc to create `data.json` for SPECTER
    metadata_read_pool = multiprocessing.Pool(processes=args.num_processes)
    metadata_read_results = []

    for i in range(SHARDS_TOTAL_NUM):
        metadata_read_results.append(
            metadata_read_pool.apply_async(
                parse_metadata_shard, args=(i, args.fields_of_study)))

    metadata_read_pool.close()
    metadata_read_pool.join()

    print("Combining all the metadata from all the shards...")
    citation_data_direct = {}
    citation_data_direct_by_shard = []
    safe_paper_ids = {}
    query_paper_ids_all_shard = []
    query_paper_ids_by_field_all_shard = []
    paper_titles = {}

    for r in tqdm.tqdm(metadata_read_results):
        citation_data_by_shard, query_paper_ids, query_paper_ids_by_field, safe_ids, titles = r.get()

        citation_data_direct.update(citation_data_by_shard)

        citation_data_direct_by_shard.append(citation_data_by_shard)

        query_paper_ids_all_shard.append(query_paper_ids)

        query_paper_ids_by_field_all_shard.append(query_paper_ids_by_field)

        safe_paper_ids.update(safe_ids)

        paper_titles.update(titles)

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    # Remove invalid papers from citation_data_direct
    print("Remove invalid papers from citation_data_direct...")
    query_paper_ids_all_shard_sanitized = {}
    query_paper_ids_by_field_all_shard_sanitized = {}

    citation_data_final = {}

    sanitize_direct_pool = multiprocessing.Pool(processes=args.num_processes)
    sanitize_direct_results = {}

    if args.shards:
        sanitize_direct_shards_list = args.shards
    else:
        sanitize_direct_shards_list = list(range(SHARDS_TOTAL_NUM))

    for i in sanitize_direct_shards_list:
        sanitize_direct_results[i] = sanitize_direct_pool.apply_async(sanitize_citation_data_direct, args=(i,))

    sanitize_direct_pool.close()
    sanitize_direct_pool.join()

    for i in tqdm.tqdm(sanitize_direct_shards_list):
        citation_data_by_shard_sanitized, query_paper_ids_sanitized, query_paper_ids_by_field_sanitized = sanitize_direct_results[i].get()

        citation_data_final.update(citation_data_by_shard_sanitized)

        query_paper_ids_all_shard_sanitized[i] = query_paper_ids_sanitized

        query_paper_ids_by_field_all_shard_sanitized[i] = query_paper_ids_by_field_sanitized

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    # Write citation_data_final to a file.
    print("Writing data.json to a file.")

    pathlib.Path(args.save_dir).mkdir(exist_ok=True)

    output_file = open(os.path.join(args.save_dir, "data.json"), 'w+')

    json.dump(citation_data_final, output_file, indent=2)

    output_file.close()

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    # Train-validation-test split
    print("Creating train-validation-test splits.")

    train_file = open(os.path.join(args.save_dir, "train.txt"), 'w+')
    val_file = open(os.path.join(args.save_dir, "val.txt"), 'w+')
    test_file = open(os.path.join(args.save_dir, "test.txt"), 'w+')

    train_file_ids_written = collections.defaultdict(bool)
    val_file_ids_written = collections.defaultdict(bool)
    test_file_ids_written = collections.defaultdict(bool)

    if args.shards:
        query_paper_ids_by_field_shards_list = args.shards
    else:
        query_paper_ids_by_field_shards_list = list(range(SHARDS_TOTAL_NUM))

    # dictionary mapping s2orc id to mag field list
    mag_fields_by_query_paper_ids = {}
    mag_fields_by_query_paper_ids['train'] = collections.defaultdict(list)
    mag_fields_by_query_paper_ids['val'] = collections.defaultdict(list)
    mag_fields_by_query_paper_ids['test'] = collections.defaultdict(list)

    for s in tqdm.tqdm(query_paper_ids_by_field_shards_list):
        for field in query_paper_ids_by_field_all_shard_sanitized[s].keys():
            field_paper_ids = query_paper_ids_by_field_all_shard_sanitized[s][field]

            random.shuffle(field_paper_ids)

            val_size = int(len(field_paper_ids) * args.val_proportion)
            test_size = int(len(field_paper_ids) * args.test_proportion)

            if args.train_proportion:
                train_size = int(len(field_paper_ids) * args.train_proportion)
            else:
                train_size = len(field_paper_ids) - val_size - test_size

            for paper_id in field_paper_ids[0:train_size]:
                if not train_file_ids_written[paper_id]:
                    train_file.write(paper_id + '\n')
                    train_file_ids_written[paper_id] = True
                mag_fields_by_query_paper_ids['train'][paper_id].append(field)

            for paper_id in field_paper_ids[train_size:train_size+val_size]:
                if not val_file_ids_written[paper_id]:
                    val_file.write(paper_id + '\n')
                    val_file_ids_written[paper_id] = True
                mag_fields_by_query_paper_ids['val'][paper_id].append(field)

            for paper_id in field_paper_ids[train_size+val_size:train_size+val_size+test_size]:
                if not test_file_ids_written[paper_id]:
                    test_file.write(paper_id + '\n')
                    test_file_ids_written[paper_id] = True
                mag_fields_by_query_paper_ids['test'][paper_id].append(field)

    train_file.close()
    val_file.close()
    test_file.close()

    print("Writing mag_fields_by_paper_ids to a file.")
    mag_fields_by_query_paper_ids_output_file = open(os.path.join(args.save_dir, "mag_fields_by_query_paper_ids.json"), 'w+')

    json.dump(mag_fields_by_query_paper_ids, mag_fields_by_query_paper_ids_output_file)

    mag_fields_by_query_paper_ids_output_file.close()

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    # Get all paper ids and dump them to a file as well.
    print("Getting all paper ids ever appearing in data.json.")
    all_paper_ids = get_all_paper_ids(citation_data_final)

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    print("Writing all paper ids to a file.")
    all_paper_ids_output_file = open(os.path.join(args.save_dir, "paper_ids.json"), 'w+')

    json.dump(all_paper_ids, all_paper_ids_output_file)

    all_paper_ids_output_file.close()

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    print("Getting MAG fields information for all paper ids.")
    metadata_mag_field_pool = multiprocessing.Pool(processes=args.num_processes)
    metadata_mag_field_results = []

    for i in range(SHARDS_TOTAL_NUM):
        metadata_mag_field_results.append(
            metadata_mag_field_pool.apply_async(
                parse_metadata_get_mag_shard, args=(i,)
            )
        )

    metadata_mag_field_pool.close()
    metadata_mag_field_pool.join()

    metadata_mag_fields = {}

    for r in tqdm.tqdm(metadata_mag_field_results):
        metadata_mag_fields.update(r.get())

    # Write metadata to a file.
    print("Writing the MAG field information...")
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
    metadata_mag_fields_output_file = open(os.path.join(args.save_dir, "mag_fields_by_all_paper_ids.json"), 'w+')

    json.dump(metadata_mag_fields, metadata_mag_fields_output_file)

    metadata_mag_fields_output_file.close()

    print("Writing safe paper ids to a file.")
    safe_paper_ids_output_file = open(os.path.join(args.save_dir, "safe_paper_ids.json"), 'w+')

    json.dump(safe_paper_ids, safe_paper_ids_output_file)

    safe_paper_ids_output_file.close()

    # Call Python GC in between steps to mitigate any potential OOM craashes
    gc.collect()

    print("Writing all paper titles to a file.")
    all_titles_output_file = open(os.path.join(args.save_dir, "titles.json"), 'w+')

    json.dump(paper_titles, all_titles_output_file, indent=2)

    all_titles_output_file.close()
