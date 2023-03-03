""" Retrieve and store phenotype data from UK Biobank. """

import argparse
import re

import pandas as pd

import dxpy
import dxdata

from util import field_names_for_ids


# def get_pheno_fields(pheno):
# 	if pheno == 'ldl':
# 		pheno_fields = [
# 			'30780', # LDL direct
# 			'6153',  # medication  (female) (1 = Cholesterol lowering medication)
# 			'6177',  # medication  (male)    
# 		]
# 	else:
# 		raise ValueError('Invalid phenotype: {}'.format(pheno))
	
# 	return pheno_fields


def get_pheno_df(participant_db, pheno, excluded_samples=None):
	"""Retrieves phenotype data from UK Biobank for a given phenotype.
	Returns as Pandas DataFrame.

	Args:
		participant_db: UK Biobank participant database.
		pheno (str): Phenotype and associated fields to retrieve. One of:
			'ldl': LDL cholesterol. Will include information on
				cholesterol medication (cholesterol_med) use and
				include an adjusted (for medication use) LDL
				cholesterol value (LDL_adj) as well as a raw LDL
				cholesterol value (LDL).
		excluded_samples (set): Set of sample IDs to exclude. If None,
			all samples will be included.
	"""

	if pheno == 'ldl':
		pheno_fields = [
			'30780', # LDL direct
			'6153',  # medication  (female) (1 = Cholesterol lowering medication)
			'6177',  # medication  (male)    
		]
	else:
		raise ValueError('Invalid phenotype: {}'.format(pheno))
	
	field_names = ['eid'] + field_names_for_ids(
		pheno_fields, participant_db
	)

	# Connect to Spark, retrieve data, and convert to DataFrame
	df = participant_db.retrieve_fields(
		names=field_names,
		coding_values='replace',
		engine=dxdata.connect(),
	)
	df = df.toPandas()

	# Modify eid/s ID col
	df = df.rename(columns={'eid':'s'})
	df['s'] = df['s'].astype(int)

	# Optional filtering of excluded samples
	if excluded_samples is not None:
		print("Filtering out excluded samples...")
		df = df[~df['s'].isin(excluded_samples)]

	# Transform features
	if pheno == 'ldl':
		df = df.rename(columns=lambda x: re.sub('p30780_i','LDL_', x))

		# Use LDL_0 as LDL, drop LDL_1, and drop where LDL is missing
		df['LDL'] = df['LDL_0']
		df.drop(['LDL_0', 'LDL_1'], axis=1, inplace=True)
		df.dropna(subset=['LDL'], inplace=True)

		df['p6153_i0'] = df['p6153_i0'].astype(str)
		df['p6177_i0'] = df['p6177_i0'].astype(str)

		# Add new cholesterol_med column (1 = Cholesterol lowering medication)
		df['cholesterol_med'] = (
			df['p6153_i0'].str.contains(
				'Cholesterol lowering medication',
				case=False,
				na=False
			) |
			df['p6177_i0'].str.contains(
				'Cholesterol lowering medication',
				na=False
			)
		).astype(int)

		# Drop medication columns
		df.drop(df.filter(regex='p6153|p6177').columns, axis=1, inplace=True)

		# Add LDL_adj column
		df['LDL_adj'] = df['LDL']
		df.loc[df['cholesterol_med'] == 1, 'LDL_adj'] = (
			df.loc[df['cholesterol_med'] == 1, 'LDL'] / 0.7
		)

		print(df.cholesterol_med.value_counts())

	return df


if __name__ == '__main__':
	"""
	Retrieve and store phenotype data from UK Biobank.
	
	Args:
		-e, --excluded_samples: Path to file containing TSV of excluded
			samples (such that each line contains a single sample ID).
		-p, --phenotype: Phenotype and associated fields to retrieve.
			One of:
				'ldl': LDL cholesterol. Will include information on
					cholesterol medication (cholesterol_med) use and
					include an adjusted (for medication use) LDL
					cholesterol value (LDL_adj) as well as a raw LDL
					cholesterol value (LDL).
		-o, --output: Path to output file. default: {phenotype}.tsv
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
		'-p', '--phenotype',
		help='Phenotype and associated fields to retrieve. One of:'
			 ' "ldl": LDL cholesterol. Will include information on'
			 ' cholesterol medication (cholesterol_med) use and'
			 ' include an adjusted (for medication use) LDL'
			 ' cholesterol value (LDL_adj) as well as a raw LDL'
			 ' cholesterol value (LDL).',
		required=True,
		choices=['ldl'],
	)
	parser.add_argument(
		'-o', '--output',
		help='Path to output file. default: {phenotype}.tsv',
		required=False,
		default=None,
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

	# Load excluded samples
	if args.excluded_samples is not None:
		excluded_samples = set(
			pd.read_csv(
				args.excluded_samples, sep='\t', header=None
			)[0].unique()
		)
	else:
		excluded_samples = None

	# Get phenotype data
	df = get_pheno_df(
		participant_db,
		args.phenotype, 
		excluded_samples
	)

	# Save to file
	if args.output is None:
		args.output = '{}.tsv'.format(args.phenotype)
	
	df.to_csv(args.output, sep='\t', index=False)

	# Print Count of people with meds
	print(df.cholesterol_med.value_counts())