import logging
import os
import re
import signal

LOGGING = {
    "level":  logging.INFO,
    "format": "[%(asctime)s %(process)d] %(levelname)s  %(message)s",
    "directory": "/log/"
}

TARGET_CONN = {
    'MYSQL_TARGET_1': {
        'host': '127.0.0.1',
        'port': 3306,
        'db': 'rover',
        'user': 'importuser',
        'password': "IluvY0u2"
    },
    'MYSQL_TARGET_2': {
        'host': 'rovermysql.cmabkwtwf8dr.us-west-2.rds.amazonaws.com',
        'port': 3306,
        'db': 'rover_uk',
        'user': 'importuser',
        'password': "<%= scope.lookupvar('auth_h::mysql::importuser') %>"
    },
    'POSTGRES_TARGET_1': {
        'host': 'roverpostgres.cmabkwtwf8dr.us-west-2.rds.amazonaws.com',
        'port': 3306,
        'db': 'financedb',
        'user': 'importuser',
        'password': "<%= scope.lookupvar('auth_h::mysql::importuser') %>"
    },
    'IMPORT_LOGGING': {
        'host': '127.0.0.1',
        'port': 3306,
        'db': 'rover',
        'user': 'importuser',
        'password': "IluvY0u2"
    },
}


IMPORT_TOPICS = {
    'pet_license': {
        'topic_name':'pet_license',
        'source_type': 'socrata',
        'source_url': 'data.seattle.gov',
        'dataset_name': 'jguv-t9rb',
        'target_type': 'mysql',
        'target_conn': 'MYSQL_TARGET_1',
        'target_table': 'pet_license',
        'target_string_columns': 'pet_license_number, animal_s_name, species, primary_breed, secondary_breed, zip_code',
        'target_date_columns': 'license_issue_date',
        'filter_soql': "where license_issue_date > '%s'",
        'filter_sql': "SELECT MAX(license_issue_date) FROM pet_license;",
        'filter_sql_result_datatype': "datetime"
    },
    'example_topic2': {
        'topic_name':'fake_pet_tax',
        'source_type': 'socrata',
        'source_url': 'data.seattle.gov',
        'dataset_name': 'xyza-f8tt',
        'target_type': 'postgres',
        'target': 'POSTGRES_TARGET_1',
        'target_table': 'tax_collected',
        'target_columns': 'col1, col2'
    }
}


SERIALIZER_SETTINGS = {
            'utf8_sanitize': True,
            'null_value': '',
            'separator': ',',
            'escapes': frozenset(['\\', '"', '\t', '\n']),
            'quotes': '"',
            'utf16_only': False
        }


MISC = {
    'quit_signal': signal.SIGTERM,
    'tmp_dir':'/mnt/c/Temp/',
    'import_log_table': 'import_log',
    'row_chunk_size': 1000,
    'pull_row_limit': 1000
}

SQL_CMDS = {
    'import_log_sql': "INSERT INTO import_log(source_file,target_host,target_db,target_table,total_row_count,chunk_row_count, comments) VALUES('%(source_file)s','%(target_host)s','%(target_db)s','%(target_table)s',%(total_row_count)s,%(chunk_row_count)s,'%(comments)s')"
}
