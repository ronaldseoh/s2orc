# Multi-domain SPECTER/SciDocs data preparation with S2ORC

Please also take a look at [the original S2ORC README](README_original.md) for the introduction to S2ORC. 

The Python scripts currently present in this repository are written to parse the S2ORC data files into [the format that SPECTER's preprocessing code expects](https://github.com/allenai/specter#advanced-training-your-own-model). Note that the SPECTER authors have not released the instructions for generating the exact set of training data used to produce their results, although they have provided the pickled data files [here](https://github.com/allenai/specter/issues/2).

## SPECTER training data

In a nutshell, SPECTER needs two sets of data:

1. `data.json`: The dictionary of the query paper IDs, and the IDs of the associated papers that either
    1. directly cited by the query paper or
    2. *indirectly* cited by the query paper, i.e. may have been cited by the directly cited paper, but *not* by the query paper.

2. `metadata.json`: titles and abstracts of all the papers ever appearing in `data.json`.

In addition, we also need to create `train.txt`, `val.txt`, `test.txt`, which needs to contain just the IDs of the query papers to be included in training, validation, and test set respectively.

Nearly all the pieces of information we need to extract are included in the `metadata` part of S2ORC. However, the `abstract` field in `metadata` appears to be incomplete, as they are provided by the sources they got the papers from, rather than directly parsing the actual paper PDFs. Hence, we will be using `pdf_parses` as well to extract the abstracts of all the papers in `data.json` and put them into `metadata.json`.

In addition, both `metadata` and `pdf_parses` are sharded into 100 gziped JSONL files. They were splitted randomly, so we would have to iterate through all the shards in order to find the paper of particular ID.

### How to run

#### First, run `specter_prep_data.py` to create `data.json`.

1. We first call `parse_metadata_shard()` for each metadata shards to obtain the following objects:
    - `output_citation_data`: the citation graph encoded in this shard file.
        - Note that the citations here currently may have *unsafe* citations, since we are yet to see the metadata of each cited paper that could exist in a different shard.
    - `output_query_paper_ids`: all *query* paper ids in this shard.
    - `output_query_paper_ids_by_field`: query paper ids organized by `mag_field_of_study`. This is needed particularly when we create a train/val/test split later on.
    - `output_safe_paper_ids`: Mappings between every signle paper ids ever found to be *safe* (have valid `mag_field_of_study`, `pdf_parse`, `pdf_parse_abstract`) and their shard #s. *Unsafe* papers will have the shard number of `-1`.
    - `output_titles`: the titles of all paper ids.
2. For each items in the step above, we combine across all the shards to create single objects.
3. With all the items returned from each shard put together, we now have `citation_data` for the entirety of s2orc, but this currently have *unsafe* citations that we have discussed above. Hence We call `sanitize_citation_data_direct` to remove them.
    - After removing unsafe citations, some query papers will be left with 0 citations. We need to remove these query papers as well.
    - `query_paper_ids` and `query_paper_ids_by_fields` also need to be updated accordingly.
4. Next, we will get all the indirect citations by calling `get_indirect_citations` for each shard.
    - For each query paper id, we call `get_citation_by_ids` to get the papers cited by them.
    - If the citations returned are safe and not cited by the query paper, we record them as indirect citations.
5. We combine direct citations and indirect citations into one single citation graph (`citation_data_final`).
6. We dump `citation_data_final` to `data.json`.
7. We create a train-val-test split from the list of query paper ids.
    - In order to make sure that each fields of study are similarly represented in the splits, we select the set proportion of papers from each list of papers by fields.
8. Lastly, we dump the following into files to run `specter_prep_metadata.py`:
    - `paper_ids.json`: all paper ids ever appearing as query papers or citations in `citation_data_final`
    - `safe_paper_ids.json`: While we used this to filter out unsafe papers, this also can tell which shard each paper id belongs to. Note that this is not the same as `paper_ids.json`: This would also contain safe papers that are **NOT** part of `paper_ids.json`.
    - `titles.json`: A dictionary of title strings for all papers. 

Once we confirm that `specter_prep_data.py` ended without errors, then we can proceed to the next part with `specter_prep_metadata.py`.

#### Second, run `specter_prep_metadata.py` to create `metadata.json`.

1. For each paper ids in `all_paper_ids`, we check `safe_paper_ids` to check which shard # they belong to and record them in `all_paper_ids_by_shard`.
2. We then call `parse_pdf_parses_shard` for each `pdf_parses` shard to extract abstracts of the papers that appear in `all_paper_ids_by_shard`. If the paper currently encoutered does appear in `all_paper_ids`, then we record the abstract to `output_metadata`, along with the titles that had already been extracted in `titles.json`.
3. We dump `metadata` to `metadata.json`.


## Multi-SciDocs `cite` and `co-cite` dataset

Please run the following commands, one by one. You will want to adjust `num_processes` based on the number of cores available on your system:

### `cite`


```bash
python3 scidocs-cite_prep_part1.py ../new/20200705v1/full/ scidocs-shard7 --num_processes 24 --shards 7
```

```bash
python3 scidocs-cite_prep_part2.py scidocs-shard7/data.json scidocs-shard7/paper_ids.json scidocs-shard7/safe_paper_ids.json scidocs-shard7/titles.json ../new/20200705v1/full/ scidocs-shard7 --num_processes 24
```

```bash
python3 scidocs-cite_prep_part3.py scidocs-shard7/data_final.json scidocs-shard7/paper_ids.json scidocs-shard7/test.txt scidocs-shard7/cite/test.qrel --max_num_positives 5 --max_num_negatives 500 
```

### `co-cite`


```bash
python3 scidocs-cite_prep_part1.py ../new/20200705v1/full/ scidocs-shard7-cocite --num_processes 24 --shards 7 --cocite
```

```bash
python3 scidocs-cite_prep_part2.py scidocs-shard7-cocite/data.json scidocs-shard7-cocite/paper_ids.json scidocs-shard7-cocite/safe_paper_ids.json scidocs-shard7-cocite/titles.json ../new/20200705v1/full/ scidocs-shard7-cocite --num_processes 24
```

```bash
python3 scidocs-cite_prep_part3.py scidocs-shard7-cocite/data_final.json scidocs-shard7-cocite/paper_ids.json scidocs-shard7-cocite/test.txt scidocs-shard7-cocite/cocite/test.qrel --max_num_positives 5 --max_num_negatives 500 --cocite
```

Please feed the resulting `json` file to `embed.py` in Multi^2SPE to get paper embeddings. Then plug in both the resulting embeddings and `qrel` files into SciDocs.
