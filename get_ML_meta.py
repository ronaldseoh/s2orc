import pandas as pd
import csv
import sys

csv.field_size_limit(sys.maxsize)

meta_input = '/iesl/data/word_embedding/gorc/s2-gorc/gorc/metadata_cs.tsv'
#meta_input = '/iesl/data/word_embedding/gorc/s2-gorc/gorc/20190928/metadata/0.tsv'
arxiv_map_path = '/iesl/data/word_embedding/gorc/s2-gorc/arXiv_id_ML'
meta_output = '/iesl/data/word_embedding/gorc/s2-gorc/gorc/metadata_ml.tsv'

def remove_version(id_in):
    version_start = id_in.rfind('v')
    assert version_start>0
    return id_in[:version_start]

meta_df = pd.read_csv(meta_input, delimiter='\t', engine='python', quoting=csv.QUOTE_NONE )
arxiv_df = pd.read_csv(arxiv_map_path, delimiter='\t', engine='python')
arxiv_df.id = arxiv_df.id.apply(remove_version)
arxiv_df = arxiv_df.rename(columns={"id": "has_arxiv_id"})
meta_ml_df = meta_df.merge(arxiv_df, how='inner', on='has_arxiv_id')
print('cs {}, ml {}'.format(len(meta_df.index),len(meta_ml_df.index)))
meta_ml_df.to_csv(meta_output, sep='\t')
#print(arxiv_df)
