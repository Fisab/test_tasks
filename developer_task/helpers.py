import csv
import itertools
import os.path
import logging

filter_by_year = lambda row, year: True if row['year_award'] == str(year) or year is None else False
filter_only_winner = lambda row, winner: True if row['win'] == str(winner) or winner is None else False


def get_generator(dict_reader, batch_size, year_award=None, only_winners=None):
	"""
	:param dict_reader: iterator of CSV file (csv.DictReader)
	:param batch_size: size for each batch
	:param year_award: <int> (optional) filter by this column
	:param only_winners: <bool> (optional) filter winners, example:
		False: return losers
		True: return winners
	"""
	buf = []
	for i, row in enumerate(dict_reader):
		row = dict(row)
		if i % batch_size == 0 and i > 0 and len(buf) > 0:
			yield buf
			buf = []
		if filter_by_year(row, year_award) is True and filter_only_winner(row, only_winners) is True:
			buf.append(row)
	if len(buf) > 0:
		yield buf


# https://pypi.org/project/memory-profiler/
# @profile
def get_data(file_name, batch_size=None, delimiter=',', year_award=None, only_winners=None, limit=None):
	"""
	Method to retrieve data from CSV file.
	Support simple way (with loading all data) and chunk way
	:param file_name: path to CSV file (supposed name included)
	:param batch_size: <int> (optional), case indicated this method will return data with generator
	:param delimiter: <str> delimiter for CSV files reader
	:param year_award: <int> (optional) filter by this column
	:param only_winners: <bool> (optional) filter winners, example:
		False: return losers
		True: return winners
	:param limit: <int> (optional) how many lines read?

	:return:
		<generator> with data, if batch_size indicated
		<list> with data
		None, if file doesn't exist
	"""
	if os.path.exists(file_name) is True:
		if limit is not None:
			dict_reader = itertools.islice(csv.DictReader(open(file_name), delimiter=delimiter), limit)  # iterator
		else:
			dict_reader = csv.DictReader(open(file_name), delimiter=delimiter)  # iterator

		if batch_size is not None:
			return get_generator(dict_reader, batch_size, year_award=year_award, only_winners=only_winners)
		else:
			return [dict(row) for row in dict_reader
					if filter_by_year(dict(row), year_award) is True
					and filter_only_winner(dict(row), only_winners) is True]
	else:
		logging.error('Error occurred: No such file {0!s}'.format(file_name))
