import numpy as np
import requests
import json
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import cache
import argparse


def mul_bool(array: np.array) -> np.array:
	"""
	:param array: массив из массивов с bool значениями
	:return: перемножает все массивы из массива и возвращает один массив
	"""
	assert len(array) > 1
	row = array[0]
	for i in array[1:]:
		row = row * i
	return row


def str2bool(v: str) -> bool:
	"""
	Simple function to convert string into bool
	:param v: input arg
	:return: converted arg to bool
	"""
	if isinstance(v, bool):
		return v
	if v.lower() in ('yes', 'true', 't', 'y', '1'):
		return True
	elif v.lower() in ('no', 'false', 'f', 'n', '0'):
		return False
	else:
		raise argparse.ArgumentTypeError('Boolean value expected.')


def get_data(url: str, to_json=True, decode=True, make_cache=True) -> list:
	"""
	:param decode: decode
	:param to_json: convert string to dict (json object)
	:param url: url to downloadable zip archive
	:return: list of dicts (json object)
	"""
	try:
		resp = requests.get(url).content
	except requests.exceptions.RequestException as e:
		raise SystemExit(e)
	zipfile = ZipFile(BytesIO(resp))

	zip_files = zipfile.namelist()
	assert len(zip_files) == 1

	data = zipfile.open(zip_files[0]).read()

	if decode is True:
		data = data.decode('WINDOWS-1251')

	if to_json is True:
		data = json.loads(data)

	if make_cache is True:
		cache.make_cache(
			cache_name=url,
			cache_data=data
		)

	return data


def detect_district(address: str, regions: pd.DataFrame) -> list:
	"""
	Based on df regions detect district from address
	:param address: string for address
	:param regions: dataframe regions from data_mart.DataMart.retrieve_regions method
	:return: list of bool for indexes
	"""
	indexes = []
	for i in address.split(','):
		indexes.append(regions.Address.str.contains(i.strip()).values)
	if address.count(',') == 0:
		result_indexes = indexes[0]
	else:
		result_indexes = mul_bool(indexes)
	if regions[result_indexes].District.shape[0] == 0:
		# ничего не нашлось
		for i, addr_substr in enumerate(address.split(',')):
			if addr_substr.isnumeric() is False:
				return regions[indexes[i]].District.unique().tolist()

	return regions[result_indexes].District.unique().tolist()
