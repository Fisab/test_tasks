import os
import pickle


CACHE_PATH = 'cache.pkl'


def make_cache(cache_name: str, cache_data):
	"""
	:param cache_name: name for caching variable
	:param cache_data: data for caching
	:return:
	"""
	if os.path.exists(CACHE_PATH) is True:
		with open(CACHE_PATH, 'rb') as f:
			cache = pickle.load(f)
		cache[cache_name] = cache_data
		with open(CACHE_PATH, 'wb') as f:
			pickle.dump(cache, f)
	else:
		cache = {
			cache_name: cache_data
		}
		with open(CACHE_PATH, 'wb') as f:
			pickle.dump(cache, f)


def get_cache(cache_name: str):
	"""
	:param cache_name:
	:return: None if cache doesnt exists and cache value if it is
	"""
	if os.path.exists(CACHE_PATH) is True:
		with open(CACHE_PATH, 'rb') as f:
			cache = pickle.load(f)
		if cache_name in cache.keys():
			return cache[cache_name]


def clean_cache():
	os.remove(CACHE_PATH)

