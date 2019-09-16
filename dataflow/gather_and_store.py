#!/usr/bin/python
"""
@package gather_and_store

 Pulls JSON records from Seattle Pet License SODA API
 and inserts them into a MySQL database



 Usage: gather_and_store.py [-h] -t IMPORT_TOPIC [--initialize]

 optional arguments:
   -h, --help            show this help message and exit
   -t IMPORT_TOPIC, --import_topic IMPORT_TOPIC
                         Specify data import topic, ex. pet_license
   --initialize          To import ALL data from scratch or truncate target and
                         start over, add --initialize
"""
import argparse
import logging
from sodapy import Socrata

import settings
from settings import LOGGING
from mysql_wrapper import MySQLWrapper
from csv_serializer import CsvSerializer

class GatherAndStore():
    """
    Main class of module gather_and_store

    @function init - handles keyboard interrupt
    @function log_msg -accepts message and writes to file or console
    @function socrata_pull - pull from socrata endpoint and write rows
    @function write_rows - wrapper to loop rows and call write_row
    @function write_row - write a single row to target
    @function write_import_log - write to database import log
    @function run - logic to conduct the pull and storage of a dataset
    """
    def __init__(self, settings, options):
        self.settings = settings
        self.options = options

        self.import_topic = self.options.import_topic
        self.initialize_data = self.options.initialize

        self.topic_info = self.settings.IMPORT_TOPICS[self.import_topic]
        self.source_type = self.topic_info["source_type"]
        self.target_type = self.topic_info["target_type"]
        self.target_conn_name = self.topic_info["target_conn"]
        self.target_table = self.topic_info["target_table"]
        self.target_string_columns = self.topic_info["target_string_columns"].split(',')
        self.target_date_columns = self.topic_info["target_date_columns"].split(',')
        self.dataset_name = self.topic_info["dataset_name"]

        self.tmp_dir = self.settings.MISC['tmp_dir']
        self.import_log_table = self.settings.MISC['import_log_table']
        self.pull_row_limit = self.settings.MISC['pull_row_limit']
        self.import_logging_sql = self.settings.SQL_CMDS["import_log_sql"]

        self.SERIALIZER_SETTINGS = self.settings.SERIALIZER_SETTINGS
        self.TARGET_CONNECTION = self.settings.TARGET_CONN[self.target_conn_name]
        self.IMPORT_LOGGING = self.settings.TARGET_CONN['IMPORT_LOGGING']

        self.implog_row_values = {
            'source_file': self.dataset_name,
            'target_host': self.TARGET_CONNECTION['host'],
            'target_db': self.TARGET_CONNECTION['db'],
            'target_table': self.target_table,
            'total_row_count': 0,
            'chunk_row_count': 0,
            'comments': ''
            }

        self.source_curs = []
        self.target_conn = {}
        self.logging_conn = {}

        self.logger = logging.getLogger('gather_and_store')

        self.init()

    ## set up db connection
    def init(self):
        """
        Initialize clients used in this session
        """
        ## Initialize the serializer
        self.serializer = CsvSerializer(**self.SERIALIZER_SETTINGS)

        # target connection info
        self.target_conn['host'] = self.TARGET_CONNECTION['host']
        self.target_conn['port'] = self.TARGET_CONNECTION['port']
        self.target_conn['user'] = self.TARGET_CONNECTION['user']
        self.target_conn['password'] = self.TARGET_CONNECTION['password']
        self.target_conn['db'] = self.TARGET_CONNECTION['db']

        ## establish a database client for target
        try:
            # use different connection method based upon target_type
            if self.target_type == 'mysql':
                self.target_client = MySQLWrapper(self.logger, **self.target_conn)
            else:
                self.logger.exception("Could not find connection method for target_type %s"\
                    % self.target_type, **LOGGING)
        except Exception:
            self.logger.exception("Could not initialize connection for target_conn %s"\
                % self.topic_info["target_conn"], **LOGGING)

        # import logging connection info
        self.logging_conn['host'] = self.IMPORT_LOGGING['host']
        self.logging_conn['port'] = self.IMPORT_LOGGING['port']
        self.logging_conn['user'] = self.IMPORT_LOGGING['user']
        self.logging_conn['password'] = self.IMPORT_LOGGING['password']
        self.logging_conn['db'] = self.IMPORT_LOGGING['db']

        ## establish a database client for import logging
        try:
            self.implog_client = MySQLWrapper(self.logger, **self.logging_conn)
        except Exception:
            self.logger.exception("Could not initialize connection for the"\
                " import logging db connection.")

        ## establish a socrata client
        try:
            self.socrata_client = Socrata(self.topic_info["source_url"], None)
        except Exception:
            self.logger.exception("Could not initialize socrata client")


    def log_msg(self, msg):
        """
        Logs a message to log_file (if exists)
        or (if no log file in args) prints to console
            @param msg - message to be logged or displayed

                if self.log_file:
                    self.logger.info(msg)
                else:
                    print "INFO: " + msg
        """
        print "INFO: " + msg


    def socrata_pull(self):
        """
        Pull data from socrata endpoint and store in target
        """
        row_count = 0
        query = "select COUNT(*)"

        if self.initialize_data:
            self.log_msg("We are initializing the data which"\
                + " means we will be pulling EVERYTHING!")

            self.target_client.execute("truncate table %s;" % self.target_table)
        else:
            result = self.target_client.execute(self.topic_info['filter_sql'], ret=True)[0][0]

            if not result:
                self.logger.exception("WARMING!!! Target table is empty.  "\
                   + "Please use --initialize to initialize the data.")
                exit(0)

            if self.topic_info['filter_sql_result_datatype'] == "datetime":
                filter_column_value = result.strftime("%Y-%m-%d")
            elif self.topic_info['filter_sql_result_datatype'] == "integer":
                filter_column_value = int(result)
            else:
                filter_column_value = result

            # limit the query by filter
            query = query + '\n' + self.topic_info['filter_soql'] % filter_column_value

        # get a row count for the dataset you want
        # determines if we do paging or just pull
        # the whole thing at once
        try:
            row_count = self.socrata_client.get(self.dataset_name, query=query)[0].get("COUNT")
        except Exception:
            self.logger.exception("Could not pull dataset (%s) count with socrata client %s" \
                % (self.dataset_name, self.topic_info["source_url"]), **LOGGING)

        self.implog_row_values['total_row_count'] = row_count

        # stop processing if no rows to pull
        if int(row_count) == 0:
            self.log_msg("No rows to pull. Our Target is up-to-date with the Source dataset.")
            exit(0)
        else:
            self.log_msg("TOTAL ROW COUNT for this pull of dataset '%s': %s rows." \
                % (self.dataset_name, row_count))

        # start building the basic query
        base_query = "select *"
        if not self.initialize_data:
            base_query = base_query + '\n' + self.topic_info['filter_soql'] % filter_column_value

        # Do we have to page results?
        # OR is our row count small enough to pull in a single call?
        if int(row_count) > self.pull_row_limit:
            self.log_msg("Paging results in chunks of %s rows." % self.pull_row_limit)

            base_query = base_query + '\n' + 'limit %s' % self.pull_row_limit

            # log the beginning of the import batch
            self.implog_row_values['comments'] = "STARTING import from %s to %s.%s ..." \
                % (self.dataset_name, self.target_conn['db'], self.target_table)
            self.write_import_log()

            decrementing_row_count = row_count
            offset = 0
            query = base_query

            while offset < row_count:
                licenses = self.socrata_client.get(self.dataset_name, query=query)
                for row in licenses:
                    self.source_curs.append(row.copy())

                row_cnt_in_cursor = len(self.source_curs)

                # write the rows in the list
                self.write_rows(offset=offset)

                if decrementing_row_count < self.pull_row_limit:
                    break

                decrementing_row_count = int(row_count) - int(offset)
                offset = offset + row_cnt_in_cursor
                query = base_query + '\n' + 'offset %s' % offset
        else:
            # no need to page, just bring over what's returned
            self.log_msg("Pull all rows without paging. RowCount: %s " % row_count)

            licenses = self.socrata_client.get(self.dataset_name, query=base_query)
            for row in licenses:
                self.source_curs.append(row.copy())

            # write the rows in the list
            self.write_rows()

        # log the beginning of the import batch
        self.implog_row_values['comments'] = "SUCCESS!! COMPLETED import from %s to %s.%s ..." \
            % (self.dataset_name, self.target_conn['db'], self.target_table)
        self.write_import_log()



    def write_rows(self, offset=0):
        """
        Write the rows to target from list self.source_curs and clear the list
        @Param offset - used to inform logging message about location in cursor
        """
        self.log_msg("Sending %s rows to target." % len(self.source_curs))
        for row in self.source_curs:
            self.write_row(row)

        self.log_msg("Successfully wrote %s rows to target." % len(self.source_curs))
        self.implog_row_values['chunk_row_count'] = len(self.source_curs)

        if self.source_curs:
            comments = "Chunk import (offset=%s): %s rows for %s." % (offset,\
               len(self.source_curs), self.dataset_name)

            self.implog_row_values['comments'] = comments
            self.write_import_log()

        self.source_curs = []


    def write_row(self, row):
        """
        Write a single row of data
            @param row: The row (with key:value) from the source table to be written
        """
        key_list, value_list = [], []
        for key, value in row.iteritems():
            key_list.append(str(key))
            if value is None:
                # how to deal with NULL dates?
                if key in self.target_date_columns:
                    value = '"00-00-00 00:00:00"' ## Stupid NULL NONE EMPTY ... Thing
                else:
                    value = "DEFAULT"

                value_list.append(str(value))
            else:
                # encode special chars for mysql insert
                if key in self.target_string_columns:
                    value = self.serializer.sanitize(value)

                value = self.serializer.sanitize(value)

                # this seeks a single backslash, but because
                # this is python, we must escape the backslash
                value = str(value).replace("\\", "\\\\")
                # escaping a double quote for mysql string encoding
                value = str(value).replace('"', '\\"')
                value_list.append('"' + str(value) + '"')
        key_str = ','.join(key_list)
        value_str = ','.join(value_list)

        # write the row to the target table
        self.target_client.execute("INSERT INTO %s.%s (%s) VALUES (%s)" \
            % (self.target_conn['db'], self.target_table, key_str, value_str))


    def write_import_log(self):
        """
        Write a single row to the import log
        """
        self.implog_client.execute(self.import_logging_sql % (self.implog_row_values))


    def run(self):
        """
        Runs the methods in the class in the following order:
            socrata_pull -> write_rows (wrapper loop) -> write_row
        """
        self.log_msg("Starting module GatherAndStore... ")
        self.socrata_pull()
        self.log_msg("Module GatherAndStore successfully completed!")


## Entry point
if __name__ == '__main__':
    # check command line
    PARSER = argparse.ArgumentParser(description='import data from SOURCE and write to TARGET')
    PARSER.add_argument("-t", "--import_topic", required=True,\
        help="Specify data import topic, ex. pet_license")
    PARSER.add_argument("--initialize", default=False, action='store_true', required=False,\
        help="To import ALL data from scratch or truncate target and start over, add --initialize")
    OPTIONS = PARSER.parse_args()

    # call run()
    GatherAndStore(settings, OPTIONS).run()
