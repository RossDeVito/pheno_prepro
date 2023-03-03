#!/bin/bash

PREFIX=ldl_v0

python3 ../../pheno_prepro/make_splits.py \
	-p /mnt/project/pheno_data/phenotypes/LDL/ldl_v0.tsv \
	-c /mnt/project/pheno_data/covariates/covariates_v0.tsv \
	-e /mnt/project/pheno_data/excluded_samples.tsv \
	-o ${PREFIX}

dx upload ${PREFIX}_train.tsv \
	--path /pheno_data/phenotypes/LDL/

dx upload ${PREFIX}_val.tsv \
	--path /pheno_data/phenotypes/LDL/

dx upload ${PREFIX}_test.tsv \
	--path /pheno_data/phenotypes/LDL/
