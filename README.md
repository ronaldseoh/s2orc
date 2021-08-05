# SPECTER training data preparation with S2ORC

Please also take a look at [the original README](https://github.com/ronaldseoh/s2orc/blob/master/README_original.md) for the introduction to S2ORC. 

The Python scripts currently present in this repository are written to parse the S2ORC data files into [the format that SPECTER's preprocessing code expects](https://github.com/ronaldseoh/specter#advanced-training-your-own-model). Note that the SPECTER authors have not released the instructions for generating the exact set of training data used to produce their results, although they have provided the pickled data files [here](https://github.com/allenai/specter/issues/2).

## Overview

In a nutshell, SPECTER needs two sets of data:

1. `data.json`: The dictionary of the paper IDs, and the IDs of the associated papers that either
    1. directly cites the paper or
    2. indirectly cites the paper, i.e. cites the paper in 1 but *not* the original paper.

2. `metadata.json`: titles and abstract of all the papers ever appearing in `data.json`.

In addition, we also need to create `train.txt`, `val.txt`, `test.txt`, which needs to contain just the IDs of the papers to be included in training, validation, and test set respectively.

Nearly all the pieces of information we need to extract are included in the `metadata` part of S2ORC. However, the `abstract` field in `metadata` appears to be incomplete, as they are provided by the sources they got the papers from, rather than directly parsing the actual paper PDFs. Hence, we will be using `pdf_parses` as well to extract the abstracts of all the papers in `data.json` and put them into `metadata.json`.
