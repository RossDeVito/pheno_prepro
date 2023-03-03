""" Utility functions for the pheno_prepro module. """

# This function is used to grab all field names (e.g. "p<field_id>_iYYY_aZZZ") of a list of field IDs
def field_names_for_ids(field_ids, participant_db):
	from distutils.version import LooseVersion
	fields = []
	for field_id in field_ids:
		field_id = 'p' + str(field_id)
		fields += participant_db.find_fields(
			name_regex=r'^{}(_i\d+)?(_a\d+)?$'.format(field_id)
		)
	return sorted(
		[field.name for field in fields], 
		key=lambda n: LooseVersion(n)
	)