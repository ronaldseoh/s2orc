import argparse
import random

import ujson as json
import tqdm


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_json', help='path to data.json.')
    parser.add_argument('paper_ids_json', help='path to paper_ids.json.')
    parser.add_argument('query_paper_ids_txt', help='path to the txt file containing query paper ids.')

    parser.add_argument('save_qrel', help='path to a directory to save the processed files.')

    parser.add_argument('--seed', default=321, type=int, help='Random seed.')

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
    query_paper_ids_file.close()

    with open(args.save_qrel, 'w') as qrel_file:
        for p_id in tqdm.tqdm(query_paper_ids):
            # Outbound citations
            cites = data[p_id]["cites"]

            # Randomly select 5 positive papers
            positives = random.sample(cites, k=5)
            
            # Sample from the non-cited papers
            all_paper_ids_without_cites = all_paper_ids - set(cites)
            
            # Randomly select 25 negative papers
            negatives = random.sample(all_paper_ids_without_cites, k=25)

            for pos_id in positives:
                qrel_file.write(str(p_id) + " 0 " + str(pos_id) + " 1\n")

            for neg_id in negatives:
                qrel_file.write(str(p_id) + " 0 " + str(neg_id) + " 0\n")
