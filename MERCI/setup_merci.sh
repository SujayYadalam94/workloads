#!/bin/bash

BASE_DIR=$HOME/MERCI/data
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

sudo pip3 install tqdm

# Get the dataset
mkdir -p $BASE_DIR/1_raw_data/amazon

cd $BASE_DIR/1_raw_data/amazon
wget -O All.json.gz https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_v2/categoryFiles/All_Amazon_Review.json.gz
wget -O meta_All.json.gz https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_v2/metaFiles/All_Amazon_Meta.json.gz

cd $SCRIPT_DIR
./control_dir_path.sh amazon_All 2748

# Preprocess
cd $SCRIPT_DIR/1_preprocess/scripts
python3 amazon_parse_divide_filter.py All

# Partition
cd $SCRIPT_DIR/2_partition/scripts
./run_patoh.sh amazon_All 2748

# Build binary
cd $SCRIPT_DIR/4_performance_evaluation/
mkdir -p bin
make all
