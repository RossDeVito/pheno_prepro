#!/bin/bash

python3 ../../pheno_prepro/get_excluded_samples_ukb.py > log.log

dx upload excluded_samples.tsv \
	--path /pheno_data/excluded_samples.tsv
