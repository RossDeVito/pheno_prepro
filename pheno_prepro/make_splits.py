""" 
Read pheno data, covariates, and optionally samples to exclude
and make train/val/test splits.
"""

import argparse
import os

import numpy as np
import pandas as pd


if __name__ == '__main__':
	"""
	Read pheno data, covariates, and optionally samples to exclude
	and make train/val/test splits.

	Args:
		-p, --pheno: path to pheno data (required)
		-c, --covar: path to covariates (required)
		-e, --exclude: path to samples to exclude
		-o, --out_prefix: output file prefix (required). Splits will be
			written as <out_prefix>_train.tsv, <out_prefix>_val.tsv,
			and <out_prefix>_test.tsv.
		-d. --output_dir: output directory (default: current directory)
		-t, --train: proportion of data to use for training (default: 0.8)
		-v, --val: proportion of data to use for validation (default: 0.1)
		--allow_missing_covar: allow samples with missing covariates
			(default: False when no flag)
		--allow_missing_pheno: allow samples with missing phenotypes
			(default: False when no flag)
	"""

	parser = argparse.ArgumentParser()

	parser.add_argument('-p', '--pheno', required=True)
	parser.add_argument('-c', '--covar', required=True)
	parser.add_argument('-e', '--exclude', default=None)
	parser.add_argument('-o', '--out', required=True)
	parser.add_argument('-d', '--output_dir', default='.')
	parser.add_argument('-t', '--train', default=0.8, type=float)
	parser.add_argument('-v', '--val', default=0.1, type=float)
	parser.add_argument('--allow_missing_covar', action='store_true')
	parser.add_argument('--allow_missing_pheno', action='store_true')

	args = parser.parse_args()

	# Make sure train and val proportions sum to <1
	assert args.train + args.val < 1

	# read pheno data
	pheno = pd.read_csv(args.pheno, sep='\t')

	# read covariates
	covar = pd.read_csv(args.covar, sep='\t')

	# read samples to exclude
	if args.exclude is not None:
		exclude = pd.read_csv(args.exclude, sep='\t', header=None)
		exclude = set(exclude[0].unique())
	else:
		exclude = set()

	# remove samples to exclude
	pheno = pheno[~pheno['s'].isin(exclude)]
	covar = covar[~covar['s'].isin(exclude)]

	# remove samples with missing covariates
	if not args.allow_missing_covar:
		covar = covar.dropna()

	# remove samples with missing phenotypes
	if not args.allow_missing_pheno:
		pheno = pheno.dropna()

	# Get samples with both covariates and phenotypes
	samples = set(pheno['s'].unique()).intersection(
		set(covar['s'].unique())
	)

	# make train/val/test splits
	np.random.seed(147)
	samples = np.random.permutation(list(samples))
	n_train = int(len(samples) * args.train)
	n_val = int(len(samples) * args.val)
	n_test = len(samples) - n_train - n_val

	train = samples[:n_train]
	val = samples[n_train:n_train+n_val]
	test = samples[n_train+n_val:]

	# Print counts
	print('Train: {}'.format(len(train)))
	print('Val: {}'.format(len(val)))
	print('Test: {}'.format(len(test)))

	# Write splits
	train = pd.DataFrame({'s': train.tolist()})
	val = pd.DataFrame({'s': val.tolist()})
	test = pd.DataFrame({'s': test.tolist()})

	train.to_csv(
		os.path.join(args.output_dir, args.out + '_train.tsv'),
		sep='\t',
		index=False,
		header=False
	)
	val.to_csv(
		os.path.join(args.output_dir, args.out + '_val.tsv'),
		sep='\t',
		index=False,
		header=False
	)
	test.to_csv(
		os.path.join(args.output_dir, args.out + '_test.tsv'),
		sep='\t',
		index=False,
		header=False
	)

	

