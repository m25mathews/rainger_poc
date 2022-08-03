#!/bin/bash

export SCOPES_DIR="../../data/scopes"

echo "INITIALIZING" &&
python main.py --action initialize \
               --n_parallel_medium 1 \
               --n_parallel_small 1 \
               --n_parallel_large 1 \
               --n_parallel_soldto 1 \
               --max_scopes 5 \
               --min_dims_medium 40 \
               --max_dims_medium 200 &&

echo "GENERATING" &&
python main.py --action generate_ops_locations --pid 0 --group small &&
python main.py --action generate_ops_locations --pid 0 --group medium &&
# python main.py --action generate_ops_locations --pid 0 --group large &&
python main.py --action generate_soldto_ops_locations --pid 0 &&

echo "INITIALIZING THIRD PARTY" &&
python main.py --action initialize_dnb \
               --n_parallel_medium 1 \
               --n_parallel_small 1 \
               --n_parallel_large 1 \
               --n_parallel_huge 1 \
               --max_scopes 5 &&

python main.py --action initialize_keepstock \
               --n_parallel 1 \
               --max_scopes 5 &&

echo "ASSOCIATING" &&
python main.py --action build_associations --group medium --pid 0 &&
python main.py --action build_associations --group large --pid 0 &&
python main.py --action build_associations_dnb --group SM --pid 0 &&
python main.py --action build_associations_dnb --group MD --pid 0 &&
python main.py --action build_associations_dnb --group LG --pid 0 &&
python main.py --action build_associations_dnb --group HG --pid 0 &&
python main.py --action build_associations_keepstock --pid 0 &&

echo "CLUSTERING" &&
python main.py --action generate_parent_ops_locations --group medium --pid 0 &&
# python main.py --action generate_parent_ops_locations --group large --pid 0 &&

echo "PERSISTING" &&
python main.py --action commit_associations &&
python main.py --action commit_associations_keepstock &&
python main.py --action commit_associations_dnb
python main.py --action commit_associations_soldto_account &&
python main.py --action populate_bridge_table