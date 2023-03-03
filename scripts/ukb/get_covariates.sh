#!/bin/bash

python3 ../../pheno_prepro/get_covariates_ukb.py \
	-e /mnt/project/pheno_data/excluded_samples.tsv

dx upload covariates.tsv \
	--path /pheno_data/covariates/covariates_v0.tsv
