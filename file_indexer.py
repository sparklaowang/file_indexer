import os
import logging

class file_indexer:
    def __init__(self):
        self.tree = {}
        self.dircount = 0 
        self.filecount = 0
        self.logger = logging.getLogger()

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

    def file_proc(self, filename):
        self.filecount += 1
        self.logger.info("file proc:%s, filecount %d" % (filename, self.filecount))
    
    def dir_proc(self, dirname):
        self.dircount += 1
        self.logger.info("dir proc:%s, dircount %s" % (dirname, self.dircount))


if __name__ == "__main__":
    tst = file_indexer()
    tst.logger.setLevel(logging.INFO)
    tst.proc('/home/spark/proj')


