from types import GeneratorType
import argparse
import time
import logging

import db
import helpers


def main(file_name, year_award, batch_size, purge_all_data, create_db, only_winners, limit):
    """
    :param file_name: which file parse?
    :param year_award: <int>
    :param batch_size: <int> if it's None then upload all data, without batch
    :param purge_all_data: <bool> purge table before upload data
    :param create_db: <bool> create database if it's doesn't exists
    :param only_winners: <bool> if true then upload only winners
    :param limit: <int> (optional) how many lines read?
    """

    db_instance = db.DB('database.db', create_db=create_db)

    table_name = file_name.replace('.csv', '')
    if db_instance.check_table_exists(table_name) is False:
        sample_rows = helpers.get_data(file_name, limit=1)  # retrieve sample row, for generate schema
        assert len(sample_rows) == 1, 'Seems this file is empty; {0!s}'.format(file_name)

        extracted_schema = db_instance.extract_schema(sample_rows[0])

        db_instance.create_table(
            table_name,
            extracted_schema,
            unique=list(sample_rows[0].keys())
        )
    elif purge_all_data is True:
        db_instance.purge_table(table_name)

    data = helpers.get_data(
        file_name,
        batch_size=batch_size,
        year_award=year_award,
        only_winners=only_winners,
        limit=limit
    )

    if isinstance(data, GeneratorType) is True:
        # load data by batch
        for batch in data:
            db_instance.insert_values(table_name, batch)
    else:
        # simple load all data
        db_instance.insert_values(table_name, data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Simple script to upload data from file_name to SQLite. '
                    'Support uploading via chunks. '
                    'Existed rows at tables will be skiped')

    parser.add_argument('-f', '--file_name', type=str, required=True,
                        help='From this file script retrieve data and load to table with same name')
    parser.add_argument('--batch_size', type=int,
                        help='Batch size for uploading data to database')
    parser.add_argument('--purge_all_data', action='store_true',
                        help='Remove all data from table before upload data')
    parser.add_argument('--create_db', action='store_true',
                        help='Will create database if it\'s doesn\'t exist')
    parser.add_argument('-y', '--year_award', type=int,
                        help='Which year_award upload to database from file?')
    parser.add_argument('--only_winners', action='store_true', default=None,
                        help='Upload only winners to table')
    parser.add_argument('--limit', type=int,
                        help='How many lines read? (from top)')

    args = parser.parse_args()

    start_time = time.time()

    main(args.file_name, args.year_award, args.batch_size,
         args.purge_all_data, args.create_db, args.only_winners, args.limit)

    time_spent = time.time() - start_time
    print('Done; Time spent: {0:.3f} sec'.format(time_spent))
