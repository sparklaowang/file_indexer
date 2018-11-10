import os
import logging
import filetype
import time
from datetime import datetime
import psql_client

class file_indexer:
    def __init__(self):
        self.tree = {}
        self.dircount = 0 
        self.filecount = 0
        self.logger = logging.getLogger()
        self.typecounter = {}
        self.no_type_files = []
        self.metadata = []
        self.psqlcli = psql_client.psql_client("infrared", "spark")

    def proc(self, TOP='.'):
        to_search = [TOP] 

        while len(to_search) != 0:
            this_item = to_search.pop()
            for files in os.listdir(this_item):
                whole_path = this_item + '/' + files
                logging.info("dealing %s", whole_path)
                if os.path.isfile(whole_path):
                    self.file_proc(whole_path)
                elif os.path.isdir(whole_path):
                    to_search.insert(0, whole_path)
                    self.dir_proc(whole_path)
        self.save()
        for item in self.typecounter.keys():
            if item == None:
                print("Can't Guess: ", self.typecounter[item])
            else:
                print(item.mime, " ", item.extension, " : ", self.typecounter[item])


    def file_proc(self, filename, dotypecount=True):
        self.filecount += 1

        self.logger.info("file proc:%s, filecount %d" % (filename, self.filecount))
        meta = self.get_meta(filename)
        self.metadata.append(meta)
        self.create_db_record(meta)

        if dotypecount == False:
            pass
        else:
            thistype = filetype.guess(filename)

            if thistype == None:
                self.no_type_files.append(filename)
                self.logger.warning("Untyped file:%s" % filename)

            if thistype in self.typecounter.keys():
                self.typecounter[thistype] += 1
            else:
                self.typecounter[thistype] = 1
        
        
        return self.typecounter[thistype]
    
    def dir_proc(self, dirname):
        self.dircount += 1
        self.logger.info("dir proc:%s, dircount %s" % (dirname, self.dircount))

    def get_meta(self, filename):
        meta = {}
        slicefilename = filename[filename.rfind('/'):]
        meta.update({"name":slicefilename[-128:]})
        meta.update({"trademark":slicefilename[-128:]}) 
        # Trademark is the name which was show on front-end, filename is the real name of the file, so bascaly don't change the filename in this directory 
        mdtime = os.path.getmtime(filename)
        mdtime = datetime.utcfromtimestamp(mdtime)
        timestr = mdtime.strftime("%Y-%m-%d %H:%M:%S")
        meta.update({"mdtime": mdtime})
        meta.update({"modytime": timestr})
        meta.update({"dir":filename[0:filename.rfind('/')]})
        ftype = filetype.guess(filename)
        meta.update({"filetype": ftype.mime if ftype != None else 'Unknow'})
        meta.update({"fileext": ftype.extension if ftype != None else 'Unknow'})
        meta.update({"filesize": os.path.getsize(filename)})
        return meta

    def create_db_record(self, meta, simulation=False):
        #execstring = "INSERT INTO file_index_meta \
        #        (name, trademark, modytime, dir, filetype, filesize) VALUES \
        #        ('{name}','{trademark}','{modytime}','{dir}','{filetype}', '{filesize}');\
        #        ".format(**meta)
        #self.logger.info(execstring)
        if not simulation:
            #self.psqlcli.execute(execstring)
            self.psqlcli.cur.execute("""
                        SELECT modytime FROM file_index_meta WHERE
                        dir=%s AND name=%s;
                    """, (meta['dir'], meta['name']))
            result = self.psqlcli.cur.fetchall()
            if len(result) > 0:
                if result[0] == meta['modytime']:
                    self.logger.info("Record Already Exisit, Skipping")
                    return True

            try :
                self.psqlcli.cur.execute("""
                            INSERT INTO file_index_meta (name, trademark, modytime, dir, filetype, filesize) 
                            VALUES (%s, %s, %s, %s, %s , %s);
                        """,
                        (meta['name'], meta['trademark'], meta['mdtime'], meta['dir'], meta['filetype'], meta['filesize']))
            except Exception as exp:
                self.logger.exception("message")
                self.psqlcli.commit()

    def save(self):
        try:
            self.psqlcli.commit()
        except Exception as exp:
            self.logger.error("Error commit db change")
        self.psqlcli.close()


if __name__ == "__main__":
    tst = file_indexer()
    tst.logger.setLevel(logging.ERROR)
    tst.proc('/home/spark/Storage/Video/NSFW/AV3')

