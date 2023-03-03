""" Store covariates for UKB samples who are not excluded. """

import argparse
import os

import pandas as pd

import dxpy
import dxdata

from util import field_names_for_ids


if __name__ == '__main__':
	""" Store covariates for UKB samples who are not excluded. 
	
	Args:
		-e, --excluded_samples: Path to file containing TSV of excluded
			samples (such that each line contains a single sample ID).
		-o, --output: Path to output file. default: covariates.tsv
	"""
	
	parser = argparse.ArgumentParser()
	
	parser.add_argument(
		'-e', '--excluded_samples',
		help='Path to file containing TSV of excluded samples (such that'
			 ' each line contains a single sample ID).',
		required=False,
		default=None,
	)
	parser.add_argument(
		'-o', '--output',
		help='Path to output file. default: covariates.tsv',
		required=False,
		default='covariates.tsv',
	)
	
	args = parser.parse_args()

	# Automatically discover dispensed dataset ID and load the dataset
	dispensed_dataset_id = dxpy.find_one_data_object(
		typename='Dataset',
		name='app*.dataset',
		folder='/',
		name_mode='glob'
	)['id']
	dataset = dxdata.load_dataset(id=dispensed_dataset_id)

	print(f'Dataset ID: {dispensed_dataset_id}')

	participant_db = dataset['participant']

	# Get covariate fields
	covar_fields = [
		'22006', # White British ancestry
		'22009', # PCA
		'31',    # sex
		'21022', # age at recruitment
	]

	field_names = ['eid'] + field_names_for_ids(
		covar_fields, participant_db
	)

	# Connect to Spark, retrieve data, and convert to DataFrame
	df = participant_db.retrieve_fields(
		names=field_names,
		coding_values='replace',
		engine=dxdata.connect(),
	)
	df = df.toPandas()

	# Optional filtering of excluded samples
	if args.excluded_samples is not None:
		print("Filtering out excluded samples...")
		exclude = pd.read_csv(args.excluded_samples, sep='\t', header=None)
		exclude = set(exclude[0].unique())

		df = df.loc[~df['eid'].isin(exclude)]

	# Save covariates
	df.to_csv(args.output, sep='\t', index=False)
