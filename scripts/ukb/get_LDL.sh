#!/bin/bash

python3 ../../pheno_prepro/get_pheno_ukb.py \
	-e /mnt/project/pheno_data/excluded_samples.tsv \
	-p ldl

dx upload ldl.tsv \
	--path /pheno_data/phenotypes/ldl_v0.tsv
