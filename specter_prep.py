import os
import pathlib
import multiprocessing
import argparse
import gzip
import json

import jsonlines
import tqdm


def parse_pdf_parses_shard(data_dir, shard_num, save_dir):

    pdf_parses_file = gzip.open(
        os.path.join(data_dir, 'pdf_parses_{}.jsonl.gz'.format(shard_num)), 'rt')
    
    reader = jsonlines.Reader(pdf_parses_file)
    
    pass

# Process metadata jsonl into `data.json` as required by SPECTER.
# Need to get all the citation information.
def parse_metadata_shard(data_dir, shard_num, save_dir):
    
    output_citation_data = {}
    
    metadata_file = gzip.open(
        os.path.join(data_dir, 'metadata_{}.jsonl.gz'.format(shard_num)), 'rt')
    
    reader = jsonlines.Reader(metadata_file)
    
    for paper in tqdm.tqdm(reader.iter(skip_invalid=True)):
        # Only consider papers that have outbound citations available
        if not paper['has_outbound_citations']:
            continue
            
        if paper['paper_id'] in output_citation_data.keys():
            print("Shard {} Duplicate paper id {} found. Please check.".format(shard_num, paper['paper_id']))
        
        # Iterate through paper ids of outbound citations
        citations = {}
        
        for out_id in paper['outbound_citations']:
            citations[out_id] = {"count": 5} # 5 = direct citation

        output_citation_data[paper['paper_id']] = citations

    output_file = open(
        os.path.join(save_dir, "data_{}.json".format(shard_num)), 'w+')
    
    json.dump(output_citation_data, output_file, indent=2)
    
    output_file.close()

def main():
    
    pass

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', help='path to a directory containing `metadata` and `pdf_parses` subdirectories.')
    parser.add_argument('save_dir', help='path to a directory to save the processed files.')

    parser.add_argument('--fields_of_study', nargs='*', type=str)
    
    args = parser.parse_args()

    # create save_dir
    pathlib.Path(args.save_dir).mkdir(exist_ok=True)
