import os
import logging
import filetype
import time

class file_indexer:
    def __init__(self):
        self.tree = {}
        self.dircount = 0 
        self.filecount = 0
        self.logger = logging.getLogger()
        self.typecounter = {}
        self.no_type_files = []
        self.metadata = []

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
        for item in self.typecounter.keys():
            if item == None:
                print("Can't Guess: ", self.typecounter[item])
            else:
                print(item.mime, " ", item.extension, " : ", self.typecounter[item])


    def file_proc(self, filename, dotypecount=True):
        self.filecount += 1

        self.logger.info("file proc:%s, filecount %d" % (filename, self.filecount))
        self.metadata.append(self.get_meta(filename))


        if dotypecount == False:
            return self.filecount;
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
        meta.update({"name":filename})
        meta.update({"trademark":filename}) 
        # Trademark is the name which was show on front-end, filename is the real name of the file, so bascaly don't change the filename in this directory 
        meta.update({"modytime": os.path.getmtime(filename)})
        meta.update({"dir":filename[0:filename.rfind('/')]})
        meta.update({"filetype":filetype.guess(filename)})
        return meta


if __name__ == "__main__":
    tst = file_indexer()
    tst.logger.setLevel(logging.INFO)
    tst.proc('/home/spark/Storage/Video/NSFW/AV3')

