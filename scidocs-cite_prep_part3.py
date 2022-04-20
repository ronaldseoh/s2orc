import argparse
import random

import ujson as json
import tqdm
import collections


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_json', help='path to data.json.')
    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')
    parser.add_argument('query_paper_ids_txt', help='path to the txt file containing query paper ids.')

    parser.add_argument('save_qrel', help='path to a directory to save the processed files.')

    parser.add_argument('--seed', default=321, type=int, help='Random seed.')

    parser.add_argument('--cocite', default=False, action='store_true')

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

    query_paper_ids_file = open(args.query_paper_ids_txt, 'r')
    query_paper_ids = query_paper_ids_file.readlines()
    query_paper_ids = [i.rstrip() for i in query_paper_ids]
    query_paper_ids_file.close()

    with open(args.save_qrel, 'w') as qrel_file:
        for p_id in tqdm.tqdm(query_paper_ids):
            if args.cocite:
                counter = collections.Counter()

                cited_by = data[p_id]["cited_by"]

                for cited_by_p_id in cited_by:
                    cited_by_p_id_cites = set(data[cited_by_p_id]["cites"])

                    try:
                        cited_by_p_id_cites.remove(p_id)
                        counter.update(cited_by_p_id_cites)
                    except KeyError:
                        continue

                # Get the paper ids with max counts only
                positive_candidates = [x[0] for x in counter.most_common(max(counter.values()))]
            else:
                # Outbound citations
                positive_candidates = data[p_id]["cites"]

            # Randomly select 5 positive papers
            if len(positive_candidates) < 5:
                positives = positive_candidates
            else:
                positives = random.sample(positive_candidates, k=5)
            
            # Sample from the non-cited papers
            all_paper_ids_without_positive_candidates = all_paper_ids - set(positive_candidates)
            
            # Randomly select 25 negative papers
            negatives = random.sample(all_paper_ids_without_positive_candidates, k=25)

            for pos_id in positives:
                qrel_file.write(str(p_id) + " 0 " + str(pos_id) + " 1\n")

            for neg_id in negatives:
                qrel_file.write(str(p_id) + " 0 " + str(neg_id) + " 0\n")
