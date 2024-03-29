import os
import pathlib
import multiprocessing
import argparse
import gzip
import random
import math
import copy
import gc
import collections

import ujson as json
import tqdm


# Process metadata jsonl into `data.json` as required by SPECTER.
# Need to get all the citation information.
def parse_metadata_shard(shard_num, fields=None):

    output_citation_data = {}
    output_query_paper_ids = []
    output_query_paper_ids_by_field = {}
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

        # Fetch titles
        output_titles[paper['paper_id']] = paper['title']

        # Query papers should have outbound citations
        if not paper['has_outbound_citations']:
            pbar.update(1)
            continue

        if paper['paper_id'] in output_citation_data.keys():
            print("Metadata shard {} Duplicate paper id {} found. Please check.".format(shard_num, paper['paper_id']))
        else:
            
            if args.cross_domain and len(paper['mag_field_of_study']) < 2:
                pbar.update(1)
                continue
            
            # if args.fields_of_study is specified, only consider the papers from
            # those fields
            if fields and set(fields).isdisjoint(set(paper['mag_field_of_study'])):
                pbar.update(1)
                continue

            # Record paper_id
            output_query_paper_ids.append(paper['paper_id'])

            # Record paper_id based on mag_field_of_study
            for paper_field in paper['mag_field_of_study']:
                if fields and paper_field not in fields:
                    continue

                if paper_field not in output_query_paper_ids_by_field.keys():
                    output_query_paper_ids_by_field[paper_field] = []

                output_query_paper_ids_by_field[paper_field].append(paper['paper_id'])

            # Iterate through paper ids of outbound citations
            citations = {}

            for out_id in paper['outbound_citations']:
                citations[out_id] = {"count": 5} # 5 = direct citation

            output_citation_data[paper['paper_id']] = citations

        pbar.update(1)

    metadata_file.close()

    return output_citation_data, output_query_paper_ids, output_query_paper_ids_by_field, output_safe_paper_ids, output_titles

def get_indirect_citations(shard_num):

    citation_data_indirect = {}

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3), position=shard_num+1)

    for paper_id in query_paper_ids_all_shard_sanitized[shard_num]:
        directly_cited_ids = citation_data_final[paper_id].keys()

        citation_data_indirect[paper_id] = {}

        # Search each shards
        indirect_citations = get_citations_by_ids(directly_cited_ids)

        for indirect_id in indirect_citations:
            # This indirect citation would serve as a hard negative only if the paper_id
            # doesn't cite it in the first place.
            # Also, check whether it is in the safe_paper_ids as decided
            # by the metadata parse result (have all the necessary values populated)
            if indirect_id not in directly_cited_ids and safe_paper_ids[indirect_id] > -1:
                citation_data_indirect[paper_id][indirect_id] = {"count": 1} # 1 = "a citation of a citation"

        pbar.update(1)

    return citation_data_indirect

def sanitize_citation_data_direct(shard_num):

    # Remove all the "unsafe" papers from the shard's citation_data_direct,
    # while avoiding iterating again through all the metadata shards
    print("Removing invalid direct citations...")

    output_citation_data_direct = copy.deepcopy(citation_data_direct_by_shard[shard_num])
    output_query_paper_ids = copy.deepcopy(query_paper_ids_all_shard[shard_num])
    output_query_paper_ids_by_field = copy.deepcopy(query_paper_ids_by_field_all_shard[shard_num])

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3),
        total=len(citation_data_direct_by_shard[shard_num].keys()),
        position=shard_num+1)

    for paper_id in citation_data_direct_by_shard[shard_num].keys():
        for cited_id in citation_data_direct_by_shard[shard_num][paper_id].keys():
            if safe_paper_ids[cited_id] == -1:
                del output_citation_data_direct[paper_id][cited_id]

        pbar.update(1)

    print("Removing query ids that no longer have any direct citations.")
    query_ids_to_remove = []

    pbar = tqdm.tqdm(
        desc="#" + "{}".format(shard_num).zfill(3),
        total=len(output_citation_data_direct.keys()),
        position=shard_num+1)

    for paper_id in tqdm.tqdm(output_citation_data_direct.keys()):
        if len(output_citation_data_direct[paper_id].keys()) == 0:
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

def get_citations_by_ids(ids):

    citations = set()

    for paper_id in ids:
        try:
            # this should be accessing citation_data_direct and
            # not citation_data_final, as cited ids may or may not be
            # part of citation_data_final
            for cited_id in citation_data_direct[paper_id].keys():
                citations.add(cited_id)
        except:
            continue

    return citations

def get_all_paper_ids(citation_data):

    all_ids = set()

    for paper_id in tqdm.tqdm(citation_data.keys()):
        all_ids.add(paper_id)

        for cited_id in citation_data[paper_id].keys():
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
        default=0.2, type=float, help='proportion of the generated dataset to be reserved for validation.')

    parser.add_argument(
        '--test_proportion',
        default=0.0, type=float, help='proportion of the generated dataset to be reserved for test.')

    parser.add_argument(
        '--train_proportion',
        type=float, help='proportion of the generated dataset to be reserved for training.')

    parser.add_argument('--cross_domain', default=False, action='store_true')

    parser.add_argument('--smoothed_weighting', default=False, action='store_true')

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

    # Add indirect citations (citations by each direct citation)
    print("Adding indirect citations...")
    indirect_citations_pool = multiprocessing.Pool(processes=args.num_processes)
    indirect_citations_results = []

    if args.shards:
        indirect_citations_shards_list = args.shards
    else:
        indirect_citations_shards_list = list(range(SHARDS_TOTAL_NUM))

    for i in indirect_citations_shards_list:
        indirect_citations_results.append(
            indirect_citations_pool.apply_async(get_indirect_citations, args=(i,)))

    indirect_citations_pool.close()
    indirect_citations_pool.join()

    # Combine citation_data_direct and citation_data_indirect into a single json file.
    print("Merging direct and indirect citations...")

    for r in tqdm.tqdm(indirect_citations_results):
        indirect = r.get()

        for paper_id in indirect.keys():
            citation_data_final[paper_id].update(indirect[paper_id])

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
    mag_fields_by_paper_ids = {}
    mag_fields_by_paper_ids['train'] = collections.defaultdict(list)
    mag_fields_by_paper_ids['val'] = collections.defaultdict(list)
    mag_fields_by_paper_ids['test'] = collections.defaultdict(list)

    if args.smoothed_weighting:
        paper_ids_by_field = collections.defaultdict(list)
        paper_counts_by_field = collections.defaultdict(int)
        total_paper_count = 0

        for s in query_paper_ids_by_field_shards_list:
            for field in query_paper_ids_by_field_all_shard_sanitized[s].keys():
                field_paper_ids = query_paper_ids_by_field_all_shard_sanitized[s][field]

                paper_ids_by_field[field] += field_paper_ids
                paper_counts_by_field[field] += len(field_paper_ids)
                total_paper_count += len(field_paper_ids)

        weights_by_field = collections.defaultdict(lambda: 1)
        weights_sum = 0

        for field in paper_counts_by_field.keys():
            weights_by_field[field] = (paper_counts_by_field[field] / total_paper_count) ** 0.7
            weights_sum += weights_by_field[field]

        for field in paper_counts_by_field.keys():
            weights_by_field[field] /= weights_sum

        for field in paper_ids_by_field.keys():
            field_paper_ids = paper_ids_by_field[field]

            adjusted_field_paper_ids_size = math.floor(weights_by_field[field] * total_paper_count)

            if adjusted_field_paper_ids_size < len(field_paper_ids):
                adjusted_field_paper_ids = random.sample(field_paper_ids, adjusted_field_paper_ids_size)
            else:
                adjusted_field_paper_ids = field_paper_ids

                if adjusted_field_paper_ids_size - len(field_paper_ids) > 0:
                    oversample_count = adjusted_field_paper_ids_size - len(field_paper_ids)

                    while oversample_count > 0:
                        num_to_sample = min(oversample_count, len(field_paper_ids))
                        oversampled_papers = random.sample(field_paper_ids, num_to_sample)
                        adjusted_field_paper_ids += oversampled_papers
                        oversample_count -= len(oversampled_papers)

            val_size = int(len(adjusted_field_paper_ids) * args.val_proportion)
            test_size = int(len(adjusted_field_paper_ids) * args.test_proportion)

            if args.train_proportion:
                train_size = int(len(adjusted_field_paper_ids) * args.train_proportion)
            else:
                train_size = len(adjusted_field_paper_ids) - val_size - test_size

            for paper_id in adjusted_field_paper_ids[0:train_size]:
                if not train_file_ids_written[paper_id]:
                    train_file.write(paper_id + '\n')
                    train_file_ids_written[paper_id] = True
                mag_fields_by_paper_ids['train'][paper_id].append(field)

            for paper_id in adjusted_field_paper_ids[train_size:train_size+val_size]:
                if not val_file_ids_written[paper_id]:
                    val_file.write(paper_id + '\n')
                    val_file_ids_written[paper_id] = True
                mag_fields_by_paper_ids['val'][paper_id].append(field)

            for paper_id in adjusted_field_paper_ids[train_size+val_size:train_size+val_size+test_size]:
                if not test_file_ids_written[paper_id]:
                    test_file.write(paper_id + '\n')
                    test_file_ids_written[paper_id] = True
                mag_fields_by_paper_ids['test'][paper_id].append(field)
    else:
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
                    mag_fields_by_paper_ids['train'][paper_id].append(field)

                for paper_id in field_paper_ids[train_size:train_size+val_size]:
                    if not val_file_ids_written[paper_id]:
                        val_file.write(paper_id + '\n')
                        val_file_ids_written[paper_id] = True
                    mag_fields_by_paper_ids['val'][paper_id].append(field)

                for paper_id in field_paper_ids[train_size+val_size:train_size+val_size+test_size]:
                    if not test_file_ids_written[paper_id]:
                        test_file.write(paper_id + '\n')
                        test_file_ids_written[paper_id] = True
                    mag_fields_by_paper_ids['test'][paper_id].append(field)

    train_file.close()
    val_file.close()
    test_file.close()

    print("Writing mag_fields_by_paper_ids to a file.")
    mag_fields_by_paper_ids_output_file = open(os.path.join(args.save_dir, "mag_fields_by_paper_ids.json"), 'w+')

    json.dump(mag_fields_by_paper_ids, mag_fields_by_paper_ids_output_file)

    mag_fields_by_paper_ids_output_file.close()

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
