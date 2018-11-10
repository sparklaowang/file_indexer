import psycopg2
import logging

class psql_client:
    def __init__(self, dbname, user):
        self.logger = logging.getLogger(dbname + "logger")
        try:
            self.conn = psycopg2.connect("dbname=%s user=%s" % (dbname, user))
        except Exception as exp:
            self.logger.error("Error: connect db")

        self.cur = sel.conn.cursor()
        self.resullt = []
            


    def fetch(exec_string):
        "A very thin wrapper for execute"
        self.cur.execute(exec_string)
        self.result = self.cur.fetchall()
        return self.result




