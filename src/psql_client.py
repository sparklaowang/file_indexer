import psycopg2
import logging

class psql_client:
    def __init__(self, dbname, user):
        self.logger = logging.getLogger(dbname + "logger")
        self.logger.setLevel(logging.INFO)
        try:
            self.conn = psycopg2.connect("dbname=%s user=%s" % (dbname, user))
        except Exception as exp:
            self.logger.error("Error: connect db")

        self.cur = self.conn.cursor()
        self.result = []
            

    def execute(self, exec_string):
        "A very thin wrapper for execute"
        try:
            self.cur.execute(exec_string)
        except Exception as exp:
            self.logger.exception("message")
            self.logger.error("Error executing: %s" % exec_string)
            self.commit()

    def commit(self):
        self.conn.commit()
    def close(self):
        self.conn.close()



