# SPECTER training data preparation with S2ORC

Please also take a look at [the original S2ORC README](https://github.com/ronaldseoh/s2orc/blob/master/README_original.md) for the introduction to S2ORC. 

The Python scripts currently present in this repository are written to parse the S2ORC data files into [the format that SPECTER's preprocessing code expects](https://github.com/ronaldseoh/specter#advanced-training-your-own-model). Note that the SPECTER authors have not released the instructions for generating the exact set of training data used to produce their results, although they have provided the pickled data files [here](https://github.com/allenai/specter/issues/2).

## Overview

In a nutshell, SPECTER needs two sets of data:

1. `data.json`: The dictionary of the query paper IDs, and the IDs of the associated papers that either
    1. directly cited by the query paper or
    2. *indirectly* cited by the query paper, i.e. may have been cited by the directly cited paper, but *not* by the query paper.

2. `metadata.json`: titles and abstracts of all the papers ever appearing in `data.json`.

In addition, we also need to create `train.txt`, `val.txt`, `test.txt`, which needs to contain just the IDs of the query papers to be included in training, validation, and test set respectively.

Nearly all the pieces of information we need to extract are included in the `metadata` part of S2ORC. However, the `abstract` field in `metadata` appears to be incomplete, as they are provided by the sources they got the papers from, rather than directly parsing the actual paper PDFs. Hence, we will be using `pdf_parses` as well to extract the abstracts of all the papers in `data.json` and put them into `metadata.json`.

In addition, both `metadata` and `pdf_parses` are sharded into 100 gziped JSONL files. They were splitted randomly, so we would have to iterate through all the shards in order to find the paper of particular ID.

## How to run

1. We first need to run `specter_prep_data.py` to create `data.json`. This script will perform few different tasks in the following order:
    1. We first call `parse_metadata_shard()` for each metadata shards to obtain the following objects:
        - `output_citation_data`: the citation graph encoded in this shard file. Note that the citations here currently may have *unsafe* citations, since we are yet to see the metadata of each citing paper that may be in a different shard.
        - `output_query_paper_ids`: all *query* paper ids (not the ones that cites query papers) in this shard.
        - `output_query_paper_ids_by_field`: query paper ids organized by `mag_field_of_study`.
        - `output_safe_paper_ids`: all paper ids found *safe* (have valid `mag_field_of_study`, `pdf_parse`, `pdf_parse_abstract`)
        - `output_titles`: the titles of all paper ids.
    2. For each item in the step above, we combine across all the shards get single objects.
    3. With all the items returned from each shard put together, we now have `citation_data` for the entirety of s2orc, but this currently have *unsafe* citations  We call `sanitize_citation_data_direct` to remove 
