"""
Utilities to make MySQL calls
"""
from warnings import filterwarnings

import MySQLdb

class MySQLWrapper:

    MAX_RETRY_ATTEMPTS = 5

    def __init__(self, logger, host, port, user, password, db, autocommit=False, silent_mode=False):
        self.logger = logger
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.conn = None
        self.failure = 0
        self.autocommit = autocommit
        self.silent_mode = silent_mode
        self.init()

    def init(self):
        if self.conn:
            self.close()
        self.conn = MySQLdb.connect(host=self.host, db=self.db, user=self.user, port=self.port, passwd=self.password)
        self.logger.info("conn instance is %s" % str(self.conn))
        self.conn.autocommit = self.autocommit

    def transaction(self, queries):
        if self.autocommit:
            self.logger.error("no transaction with autocommit")
            return
        try:
            cursor = self.conn.cursor()
            for query in queries:
                cursor.execute(query)
        except:
            self.logger.exception("queries %s failed" % (' '.join(queries)))
            self.close()
            self.init()

    def execute(self, query, ret=False):
        while True:
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                if self.silent_mode != True:
                    self.logger.info("query:%s" % query)
                if not self.autocommit:
                    self.conn.commit()
                # Reset failure counter as the execution succeeded.
                self.failure = 0

                if ret:
                    return cursor.fetchall()
                else:
                    return
            except Exception, e:
                self.failure += 1
                self.logger.exception("query %s failed" % (query))
                if self.failure > self.MAX_RETRY_ATTEMPTS:
                    raise e
                self.init()

    def close(self):
        self.conn.close()
