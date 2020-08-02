import helpers
import cache

import pandas as pd
import numpy as np

from io import BytesIO


class DataMart:
	def __init__(self, via_cache):
		"""
		:param via_cache: use cache when preparing data mart
		if True then it will try to access data from cache
		if False then it will retrieve data from source and then cache it
		"""
		self.urls = {
			'childens_clinic': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=861920',
				'format': 'json',
				'description': 'Поликлиническая помощь детям'
			},
			'adult_clinic': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=887773',
				'format': 'json',
				'description': '# Поликлиническая помощь взрослым'
			},
			'social_food': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=871418',
				'format': 'json',
				'description': '# Общественное питание в г.Москве'
			},
			'education': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=888685',
				'format': 'json',
				'description': '# Образовательные учреждения города Москвы'
			},
			'wifi': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=864835',
				'format': 'xlsx',
				'description': '# Городской Wi-Fi (xlsx)'
			},
			'rescue_garrison': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=868132',
				'format': 'xlsx',
				'description': '# Данные о вызовах пожарно-спасательного гарнизона в г. Москва (xlsx)'
			},
			'regions': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=2044',
				'format': 'xlsx',
				'description': '# районы + улицы (xlsx)'
			},
			'swimming_pool': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=894596',
				'format': 'json',
				'description': 'Данные о бассейнах (крытых)'
			},
			'emergency_services': {
				'url': 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=402655',
				'format': 'xlsx',
				'description': 'Аварийные службы по ремонту водопровода, канализации, освещения и отопления в многоквартирных домах'
			}
		}

		self.regions = None
		self.via_cache = via_cache

	async def _retrieve_data(self, url_key):
		data = None
		if self.via_cache is True:
			data = cache.get_cache(self.urls[url_key]['url'])
		if data is None:
			data = await helpers.get_data(
				self.urls[url_key]['url'],
				to_json=True if self.urls[url_key]['format'] != 'xlsx' else False,
				decode=True if self.urls[url_key]['format'] != 'xlsx' else False
			)
		return data

	async def retrieve_regions(self):
		raw_regions = await self._retrieve_data('regions')
		self.regions = pd.read_excel(BytesIO(raw_regions))
		self.regions['District'] = self.regions.District.apply(lambda x: x.strip())
		return self.regions

	async def _prepare_child_clinic(self):
		clinic_ch = await self._retrieve_data('childens_clinic')
		df_clinic_ch = pd.DataFrame(clinic_ch)
		df_clinic_ch['District'] = df_clinic_ch.ObjectAddress.apply(lambda x: x[0]['District'] if len(x) != 0 else None)
		df_clinic_ch['District'] = df_clinic_ch.District.apply(lambda x: x.strip() if x is not None else None)

		self.df_clinic_ch_count = df_clinic_ch[['ShortName', 'District']].groupby('District').count() \
			.reset_index() \
			.rename({'ShortName': 'ChildClinicCount'}, axis=1)

	async def _prepare_education(self):
		educ = await self._retrieve_data('education')
		df_educ = pd.DataFrame(educ)
		educ_buf = df_educ[['FullName', 'InstitutionsAddresses']].copy()
		educ_buf['District'] = educ_buf.InstitutionsAddresses.apply(lambda x: list(set([i['District'] for i in x])))
		educ_buf = educ_buf.drop('InstitutionsAddresses', axis=1)
		# explode на пандасе <0.15
		educ_buf = pd.DataFrame({
			'FullName': educ_buf.FullName.repeat(educ_buf.District.str.len()),
			'District': np.concatenate(educ_buf.District.values)
		})
		self.educ_count = educ_buf.drop_duplicates().groupby('District').count() \
			.reset_index() \
			.rename({'FullName': 'educCount'}, axis=1)

	async def _prepare_social_food(self):
		food = await self._retrieve_data('social_food')
		df_food = pd.DataFrame(food)
		self.df_food_count = df_food[['Name', 'District']].groupby('District').count() \
			.reset_index() \
			.rename({'Name': 'FoodCount'}, axis=1)

	async def _prepare_rescue(self):
		rescue = await self._retrieve_data('rescue_garrison')
		df_rescue = pd.read_excel(BytesIO(rescue))
		df_rescue['AdmArea'] = df_rescue.AdmArea.apply(lambda x: x.strip())

		# от очепяток даже strip не спасает(

		to_repl = [
			['Северо-Востосный административный округ', 'Северо-Восточный административный округ'],
			['Северо-Закпадный административный округ', 'Северо-Западный административный округ'],
			['Северо-Заподный административный округ', 'Северо-Западный административный округ'],
			['Северно-Западный административный округ', 'Северо-Западный административный округ'],

			['Троицкий и Новомосковский  административные округа', 'Троицкий и Новомосковский административные округа'],
			['Троицкий и Новомосковский административные округ а', 'Троицкий и Новомосковский административные округа'],
			['Троицкий и Новомосковский административный округа', 'Троицкий и Новомосковский административные округа'],
			['Троицкий и Новомосковский административные  округа', 'Троицкий и Новомосковский административные округа'],

			['Юго-Востосный административный округ', 'Юго-Восточный административный округ'],
			['Юго-Восточнный административный округ', 'Юго-Восточный административный округ'],
			['Юго=Восточный административный округ', 'Юго-Восточный административный округ'],

			['Юго-западный административный округ', 'Юго-Западный административный округ'],
			['ЮгоЗападный административный округ', 'Юго-Западный административный округ'],

			['Запдный административный округ', 'Западный административный округ'],
		]

		for repl in to_repl:
			df_rescue['AdmArea'] = df_rescue.AdmArea.str.replace(repl[0], repl[1])

		if self.regions is None:
			await self.retrieve_regions()

		self.adm_distr_rescue = self.regions[['AdmArea', 'District']].drop_duplicates().set_index('AdmArea').join(
			df_rescue[['AdmArea', 'Calls']].groupby('AdmArea').sum()
		).reset_index()
		self.adm_distr_rescue.AdmArea.value_counts() \
			.to_frame() \
			.rename({'AdmArea': 'DistrictsCount'}, axis=1)

		self.adm_distr_rescue = self.adm_distr_rescue.set_index('AdmArea').join(
			self.adm_distr_rescue.AdmArea.value_counts() \
				.to_frame() \
				.rename({'AdmArea': 'DistrictsCount'}, axis=1)
		).reset_index() \
			.rename({'index': 'AdmArea'}, axis=1)

		self.adm_distr_rescue[
			'CallsDistrict'] = self.adm_distr_rescue.Calls.values / self.adm_distr_rescue.DistrictsCount.values

	async def _prepare_swimming_pools(self):
		swim_p = await self._retrieve_data('swimming_pool')
		self.df_swim_p = pd.DataFrame(swim_p)

	async def _prepare_emergency(self):
		emergency = await self._retrieve_data('emergency_services')
		self.df_emergency = pd.read_excel(BytesIO(emergency))

	async def _merge_data(self):
		"""
		Merge all dataframes to one
		:return:
		"""
		if self.regions is None:
			await self.retrieve_regions()

		df = pd.DataFrame(
			self.regions.District.unique()
		).rename({0: 'District'}, axis=1)
		self.df = df.set_index('District').join(
			self.df_clinic_ch_count.set_index('District'),
			how='left'
		).join(
			self.educ_count.set_index('District'),
			how='left'
		).join(
			self.df_food_count.set_index('District'),
			how='left'
		).join(
			self.adm_distr_rescue[['District', 'CallsDistrict', 'AdmArea', 'Calls']] \
				.rename({'Calls': 'AdmAreaCalls'}, axis=1) \
				.set_index('District'),
			how='left'
		).join(
			self.df_swim_p.District.value_counts().to_frame().rename({'District': 'SwimCount'}, axis=1),
			how='left'
		).join(
			self.df_emergency[['District', 'Name']].groupby('District').count()\
				.rename({'Name': 'emergencyCount'},axis=1),
			how='left'
		).reset_index()

		# calculate quality of life
		self.df['qualityLife'] = (self.df['ChildClinicCount'].values + self.df['educCount'].values + self.df[
			'FoodCount'].values) / self.df['CallsDistrict'].values

		self.df['qualityLifeActivity'] = (self.df['SwimCount'].values + self.df['ChildClinicCount'].values + self.df[
			'educCount'].values + self.df['FoodCount'].values) / self.df['CallsDistrict'].values

		self.df['qualityLifeA_Emergency'] = (self.df['emergencyCount'].values + self.df['SwimCount'].values + self.df[
			'ChildClinicCount'].values + self.df['educCount'].values + self.df[
			'FoodCount'].values) / self.df['CallsDistrict'].values

	async def prepare_data_mart(self):
		"""
		Run all function to retrieve data and then run func to merge it
		:return:
		"""
		# load and prepare all data
		await self._prepare_child_clinic()
		await self._prepare_education()
		await self._prepare_social_food()
		await self._prepare_rescue()
		await self._prepare_swimming_pools()
		await self._prepare_emergency()

		# then create data mart
		await self._merge_data()

	def get_data_mart(self) -> pd.DataFrame:
		"""
		Getter
		:return: data mart
		"""
		return self.df
