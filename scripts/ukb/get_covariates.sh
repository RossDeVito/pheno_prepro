#!/bin/bash

python3 ../../pheno_prepro/get_covariates_ukb.py

dx upload covariates.tsv \
	--path /pheno_data/covariates/covariates_v0.tsv
