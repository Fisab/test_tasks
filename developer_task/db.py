import sqlite3
import os.path
import logging

DATABASE_TYPES = {
	str: 'TEXT',
	int: 'INTEGER',
	bool: 'TINYINT'
}


class DB:
	def __init__(self, db_file, create_db=False):
		"""
		Class for work with database SQLite

		:param db_file: name of database
		:param create_db: if db file not found create new. Otherwise through exception and cancel
		"""
		if create_db is False:
			assert os.path.exists(db_file), 'Cant found database file'

		self.db_file = db_file

		self._conn = None
		self.cursor = None

		self._create_connection()

	def __del__(self):
		if '_conn' in dir(self) and self._conn is not None:
			self._conn.close()

	def _create_connection(self):
		"""
		Create connection and initialize cursor
		"""
		try:
			self._conn = sqlite3.connect(self.db_file)
			# я выбрал вариант sqlite (вместо postgres)
			# и вот таким образом проверяю доступность базы данных
			# в случае не локальной базы данных я бы повесил обработчики ситуации на подключение к базе
			self.cursor = self._conn.cursor()
		except Exception as e:
			logging.error(
				'Error occurred when connecting to database: {0!s}'.format(e))

	def __query(self, sql_query, fetch=False, commit=False):
		"""
		Abstract method for execute query
		:param sql_query: <str>
		:param fetch: <bool> if True then fetch result and return, otherwise return status
		"""
		assert fetch is True or commit is True, \
			'Something one of \'fetch\' or \'commit\' should be selected'

		try:
			self.cursor.execute(sql_query)
			if fetch is True:
				return self.cursor.fetchall()
			elif commit is True:
				self._conn.commit()
			return True
		except sqlite3.Error as e:
			logging.error(
				'Error occurred when executing query: {0!s}'.format(e))
			return False

	@staticmethod
	def parse_value(value, sql_format=False):
		"""
		Simple function to parse values

		:param value: <str>
		:param sql_format: <bool> if this True then method will return values in sqlite friendly format
		:return: parsed value, example:
			< input: '322'
			> output: 322
		"""
		assert isinstance(value, str), 'value must be <str> type'

		if value.isdigit() is True:
			return int(value)
		elif value.lower() in ['true', 'false']:
			if sql_format is True:
				return 1 if value.lower() == 'true' else 0
			else:
				return True if value.lower() == 'true' else False
		else:
			return "'{0!s}'".format(value.replace("'", "''"))

	def extract_schema(self, example_row):
		"""
		Extracting columns and values type for generate table schema
		:param example_row: <dict> row
		:return: <list <dict>>, example [{'name': col_name, 'type': col_type}]
		"""
		schema = []
		for key in example_row:
			schema.append({
				'name': key,
				'type': DATABASE_TYPES.get(type(self.parse_value(example_row[key])))
			})

		return schema

	def create_table(self, table_name, extracted_schema, unique=None):
		"""
		:param table_name: <str> name of table
		:param extracted_schema: <list> output of self.extract_schema,
			example: [{'name': col_name, 'type': col_type}]
		:param unique: <list> values for unique params

		:return: <bool> execution status
		"""
		if unique is None:
			unique = []
		# generate sql query
		sql_query = '''
			CREATE TABLE {0!s} (
				{1!s}{2!s}
			);
		'''.format(
			table_name,
			', '.join([' '.join(i.values()) for i in extracted_schema]),
			', UNIQUE({0!s})'.format(', '.join(unique)) if len(unique) > 0 else ''
		)
		# execute query and commit to db changes
		return self.__query(sql_query, commit=True)

	def check_table_exists(self, table_name):
		"""
		Simple function to check if table exists by it's name
		:param table_name: <str>
		:return: <bool>

		"""
		sql_query = 'SELECT name FROM sqlite_master WHERE type="table" AND name="{0!s}";'.format(table_name)
		data = self.__query(sql_query, fetch=True)
		if data is False:
			return
		return True if len(data) > 0 else False

	def insert_values(self, table_name, values):
		"""
		Method insert unique rows to table
		:param table_name: <str>
		:param values: <list <dict>>

		:return: execution status
		"""
		assert self.check_table_exists(table_name) is True, \
			'Error occurred when inserting value, no such table {0!s}'.format(table_name)

		# generate sql query
		column_names = values[0].keys()

		sql_values = []
		for row in values:
			sql_values.append('({0!s})'.format(
				', '.join(
					['{0!s}'.format(self.parse_value(value, sql_format=True))
						for value in row.values()])
			))

		sql_query = 'INSERT OR IGNORE INTO {0!s} ({1!s}) VALUES {2!s}'.format(
			table_name,
			', '.join(column_names),
			', '.join(sql_values)
		)
		# execute query and commit to db changes
		return self.__query(sql_query, commit=True)

	def purge_table(self, table_name):
		"""
		Method for purge table
		:param table_name:
		:return: execution status
		"""
		sql_query = 'DELETE FROM {0!s}'.format(table_name)
		return self.__query(sql_query, commit=True)
