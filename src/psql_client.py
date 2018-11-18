import psycopg2
import logging
try:
    from tornado.options import define, options
    define("psql_dbname", default="infrared", help="database name")
    define("psql_user", default="spark", help="database user")
except Exception as exp:
    # When start seperatelly or by fileindexer, tornado may not start
    logging.exception("message")

class psql_client:
    def __init__(self, dbname=None, user=None):
        self.logger = logging.getLogger(dbname + "logger")
        self.logger.setLevel(logging.INFO)
        if dbname == None or user == None:
           dbname = options.psql_dbname
           user = options.psql_user
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

    def id_2_path(self, fileid):
        print(fileid)
        try:
            self.cur.execute("""SELECT dir,name FROM file_index_meta
        WHERE id=%s;""", [fileid])
            result = self.cur.fetchall()
        except Exception:
            self.logger.exception("message")
            self.conn.commit()
        if len(result) == 0: # No record was found
            self.logger.Error("index to path failed: No such file")
            return(-1)
        else:
            path = result[0][0] + '/' + result[0][1]
            self.logger.info("%s->%s" % (fileid, result[0][0] +'/'+ result[0][1]))
            return(result[0][0] + result[0][1])
    def gettag(self, fileid):
        try:
            self.cur.execute("""SELECT tags FROM file_index_meta
        WHERE id=%s""",[fileid])
            result = self.cur.fetchall()
        except Exception:
            self.logger.exception("message")
        result = result[0][0]
        self.logger.info("Get TAG for %s = %s" % (fileid, result))
        return(result)

    def settag(self, fileid, newtag):
        # Newtag is an interger !!!!
        exist_tags = self.gettag(fileid)
        if int(newtag) in exist_tags:
            return(True)
        else:
            try:
                self.cur.execute("""UPDATE file_index_meta
                    SET tags[%s]=%s 
                    WHERE id=%s;""", [len(exist_tags)+1, newtag, fileid])
            except Exception:
                self.logger.exception("message")
                self.commit()
                return(False)
        self.logger.info("New TAG: %s for %s "%(newtag, fileid))
        self.commit()
        return(True)
    def deltag(self, fileid, deltag):
        try:
            self.cur.execute("""UPDATE file_index_meta
                    SET tags=array_remove(tags, %s)
                    WHERE id=%s;""", [deltag, fileid])
        except Exception:
            self.logger.exception("message")
            self.commit()
            return(False)
        return(True)




    def commit(self):
        self.conn.commit()
    def close(self):
        self.conn.close()



