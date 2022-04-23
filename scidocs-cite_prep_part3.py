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

    parser.add_argument('--max_num_positives', default=25, type=int, help='Maximum number of positive examples to include.')
    parser.add_argument('--max_num_negatives', default=50, type=int, help='Maximum number of positive examples to include.')

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

    empty_cocite_count = 0

    with open(args.save_qrel, 'w') as qrel_file:
        for p_id in tqdm.tqdm(query_paper_ids):
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

                if len(counter.values()) == 0:
                    empty_cocite_count += 1
                    continue

                # Get the paper ids until there are at least 5 papers
                frequency = max(counter.values())
                positive_candidates = []
                
                while len(positive_candidates) < args.max_num_positives and frequency > 0:
                    positive_candidates += [x[0] for x in counter.most_common(frequency)]
                    frequency -= 1
            else:
                # Outbound citations
                positive_candidates = data[p_id]["cites"]

            positive_candidates = set(positive_candidates)

            try:
                # Just in case the query paper itself is among the candidates...
                positive_candidates.remove(p_id)
            except:
                pass

            # Randomly select max_num_positives positive papers
            if len(positive_candidates) < args.max_num_positives:
                positives = positive_candidates
            else:
                positives = random.sample(positive_candidates, k=args.max_num_positives)
            
            # Sample from the non-cited papers
            negative_candidates = all_paper_ids - positive_candidates

            try:
                # Just in case the query paper itself is among the candidates...
                negative_candidates.remove(p_id)
            except:
                pass


            # Randomly select 50 negative papers
            if len(negative_candidates) < args.max_num_negatives:
                negatives = negative_candidates
            else:
                negatives = random.sample(negative_candidates, k=args.max_num_negatives)

            for pos_id in positives:
                qrel_file.write(str(p_id) + " 0 " + str(pos_id) + " 1\n")

            for neg_id in negatives:
                qrel_file.write(str(p_id) + " 0 " + str(neg_id) + " 0\n")

    print("empty_cocite_count = ", str(empty_cocite_count))
