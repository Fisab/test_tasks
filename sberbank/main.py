import helpers
import data_mart
import pandas as pd
import argparse


def calc_quality_life(address: str, regions: pd.DataFrame, dm_df: pd.DataFrame):
	"""
	:param address: string with address, something like 'Мытная, 66'
	:param regions: dataframe with info about region s
	:param dm_df: data mart dataframe
	:return: None
	"""
	district = helpers.detect_district(address, regions)
	if len(district) == 0:
		return 'Cant find this address'
	elif len(district) > 1:
		print(district, '\n Please choose ur district (type index from 0)')
		index = input()
		district = district[int(index)]
	else:
		district = district[0]
	data = dm_df[dm_df.District == district].to_dict(orient='records')[-1]
	for i in ['qualityLife', 'qualityLifeActivity', 'qualityLifeA_Emergency']:
		print('{0!s} - {1!s}'.format(i, data[i]))

	place_top = dm_df.sort_values('qualityLifeA_Emergency', ascending=False).District.tolist().index(district) + 1

	print('Ranking by quality with activity and emergency {0!s} out of {1!s}'.format(place_top, dm_df.shape[0]))


def main(address, via_cache):
	dm = data_mart.DataMart(via_cache=via_cache)
	print('Preparing data mart...')
	dm.prepare_data_mart()

	regions = dm.regions
	calc_quality_life(
		address=address,
		regions=regions,
		dm_df=dm.get_data_mart()
	)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description='Script for calculate quality of life by your address'
					'Support generate and use cache'
	)

	parser.add_argument('-a', '--address', type=str, required=True,
						help='For this address script will calculate quality of life'
							 'If it contains more informat like ("Домодедовская, 42") please write this arg in quotas')
	parser.add_argument('-c', '--via_cache', type=helpers.str2bool, default=True,
						help='Use cache? If yes and cache doesnt exists script try to access file from source')

	args = parser.parse_args()
	main(args.address, args.via_cache)
