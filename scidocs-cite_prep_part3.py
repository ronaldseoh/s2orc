import argparse
import random
import multiprocessing

import ujson as json
import tqdm
import collections


def remove_negative_candidates(process_num):
    cum_count_before_this_process = int(len(negative_candidates_temp) / args.num_processes) * (process_num)

    if process_num == args.num_processes - 1:
        cum_count_until_this_process = len(negative_candidates_temp)
    else:
        cum_count_until_this_process = int(len(negative_candidates_temp) / args.num_processes) * (process_num + 1)

    indexes_to_process = range(cum_count_before_this_process, cum_count_until_this_process)

    filtered_ids = []

    # Filter based on MAG field information
    for i in indexes_to_process:
        nc = negative_candidates_temp[i]

        try:
            nc_mag_fields = set(mag_fields_by_all_paper_ids[nc])
        except:
            continue

        if nc_mag_fields.isdisjoint(p_id_mag_fields):
            filtered_ids.append(nc)

    return filtered_ids

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_json', help='path to data.json.')
    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')
    parser.add_argument('mag_fields_by_all_paper_ids_json', help='path to mag_fields_by_all_paper_ids.json.')
    parser.add_argument('query_paper_ids_txt', help='path to the txt file containing query paper ids.')

    parser.add_argument('save_qrel', help='path to a directory to save the processed files.')

    parser.add_argument('--seed', default=321, type=int, help='Random seed.')

    parser.add_argument('--cocite', default=False, action='store_true')

    parser.add_argument('--num_processes', default=10, type=int, help='Number of processes to use.')

    args = parser.parse_args()

    # Random seed fix for Python random
    random.seed(args.seed)

    data_file = open(args.data_json, 'r')
    data = json.load(data_file)
    data_file.close()

    # Load paper_ids.json
    print("Loading paper_ids.json...")
    all_paper_ids_file = open(args.paper_ids_json, 'r') 
    all_paper_ids = set(json.load(all_paper_ids_file))
    all_paper_ids_file.close()

    print("Loading mag_fields_by_all_paper_ids.json...")
    mag_fields_by_all_paper_ids_file = open(args.mag_fields_by_all_paper_ids_json, 'r') 
    mag_fields_by_all_paper_ids = json.load(mag_fields_by_all_paper_ids_file)
    mag_fields_by_all_paper_ids_file.close()

    query_paper_ids_file = open(args.query_paper_ids_txt, 'r')
    query_paper_ids = query_paper_ids_file.readlines()
    query_paper_ids = [i.rstrip() for i in query_paper_ids]
    query_paper_ids_file.close()

    empty_cocite_count = 0

    with open(args.save_qrel, 'w') as qrel_file:
        for p_id in tqdm.tqdm(query_paper_ids):
            if p_id not in mag_fields_by_all_paper_ids.keys():
                continue

            p_id_mag_fields = set(mag_fields_by_all_paper_ids[p_id])

            if args.cocite:
                counter = collections.Counter()

                cited_by = data[p_id]["cited_by"]

                for cited_by_p_id in cited_by:
                    try:
                        cited_by_p_id_cites = set(data[cited_by_p_id]["cites"])
                        counter.update(cited_by_p_id_cites)
                        del counter[p_id]
                    except KeyError:
                        continue

                # Get the paper ids until there are at least 5 papers
                if len(counter.values()) == 0:
                    empty_cocite_count += 1
                    continue

                frequency = max(counter.values())
                positive_candidates = []
                
                while len(positive_candidates) < 5 and frequency > 0:
                    positive_candidates += [x[0] for x in counter.most_common(frequency)]
                    frequency -= 1
            else:
                # Outbound citations
                positive_candidates = data[p_id]["cites"]

            positive_candidates = set(positive_candidates)

            # Randomly select 5 positive papers
            if len(positive_candidates) < 5:
                positives = positive_candidates
            else:
                positives = random.sample(positive_candidates, k=5)
            
            # Sample from the non-cited papers
            negative_candidates_temp = list(all_paper_ids - positive_candidates)

            print("filtering negative candidates...")
            negative_candidates_filter_pool = multiprocessing.Pool(processes=args.num_processes)
            negative_candidates_filter_results = []

            for np in range(args.num_processes):
                negative_candidates_filter_results.append(
                    negative_candidates_filter_pool.apply_async(
                        remove_negative_candidates, args=(np,)
                    )
                )

            negative_candidates_filter_pool.close()
            negative_candidates_filter_pool.join()

            negative_candidates = []

            for r in negative_candidates_filter_results:
                result = r.get()
                negative_candidates += result

            # Randomly select 50 negative papers
            if len(negative_candidates) < 50:
                negatives = negative_candidates
            else:
                negatives = random.sample(negative_candidates, k=50)

            for pos_id in positives:
                qrel_file.write(str(p_id) + " 0 " + str(pos_id) + " 1\n")

            for neg_id in negatives:
                qrel_file.write(str(p_id) + " 0 " + str(neg_id) + " 0\n")

    print("empty_cocite_count = ", str(empty_cocite_count))
