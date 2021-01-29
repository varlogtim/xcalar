import uuid
import random
import optparse
from random import randint
import string
import os
import json
import pyarrow as pa
import string

class MdmFiles:
    def __init__(self, numSchemas=20, numFiles=10000, minRows=10, maxRows=20, baseDir='mdmdemo', depth=8, seed=10, largeFile=False):
        self.numSchemas = numSchemas
        self.numFiles = numFiles
        self.minRows = minRows
        self.maxRows = maxRows
        self.baseDir = baseDir
        self.depth = depth
        self.alphabet = list(string.ascii_uppercase)
        self.paths = []
        self.bad_ext = 2
        self.no_ext = 3
        self.minDir = 20
        self.maxDir = 50
        self.numExt = 3
        self.baseCols = 2
        self.trailCols = 2
        self.largeFile = largeFile
        random.seed(seed)

    def gen_dirs(self):
        for path in range(random.randint(self.minDir, self.maxDir)):
            fullpath = []
            fullpath.append(self.baseDir)
            for level in range(random.randint(1, self.depth)):
                fullpath.append(str(uuid.uuid4())[:-29])
            fullpath = '/'.join(fullpath)
            os.makedirs(fullpath)
            self.paths.append(fullpath)

    def malform(self, mystr):
        mino, maxo = sorted([random.randint(0, len(mystr)), random.randint(0, len(mystr))])
        return mystr[mino:maxo]

    def gen_schema_files(self):
        base_hdr = random.sample(self.alphabet, randint(self.baseCols, self.baseCols+3))
        self.gen_dirs()
        estNumFiles = int(self.numFiles/(self.numSchemas*self.numExt))
        lFile = False
        for f_ind in range(estNumFiles):
            if self.largeFile and f_ind > (estNumFiles - random.randint(1, 3)):
                lFile = True
                self.maxRows = random.randint(100000000, 300000000)
            csvdata = []
            jsondata = []
            csvheader = jsonheader = None
            hdr = base_hdr
            # if you want trailing
            if random.randint(1,2) == 2:
                trail_hdr = random.sample(self.alphabet, randint(self.trailCols, self.trailCols+2))
                hdr = base_hdr + trail_hdr
            # if you want to shuffle schemas/super schemas
            if random.randint(1, 2) == 2:
                random.shuffle(hdr)
            num_cols = len(hdr)
            csvheader = ','.join(hdr)
            lateschema = largerec = malformed = emptyfile = False
            jsonheader = hdr
            if random.randint(1, 2) == 2:
                lateschema = True
                jsonheader = base_hdr
            if random.randint(1, 30) == 11:
                largerec = True
            if random.randint(1, 20) == 11:
                malformed = True
            if random.randint(1, 50) == 11:
                emptyfile = True
            csvdata.append(csvheader)
            dfdata = [[] for col in range(len(hdr))]
            numRows = random.randint(self.minRows, self.maxRows)
            for rind in range(numRows):
                row = []
                jsonrecord = {}
                csvrecord = None
                for cind in range(num_cols):
                    offset = random.randint(28, 32)
                    val = str(uuid.uuid4())[:-offset]
                    row.append(val)
                    if largerec:
                        dfdata[cind].append(10000*val) # blow it up
                    else:
                        dfdata[cind].append(val)
                csvrecord = ','.join(row)
                csvdata.append(csvrecord)
                if rind > (numRows/2) and lateschema:
                    jsonheader = hdr
                for cind in range(len(jsonheader)):
                    jsonrecord[jsonheader[cind]] = row[cind]
                jdump = json.dumps(jsonrecord)
                if malformed:
                    jdump = self.malform(jdump)
                jsondata.append(jdump)
            path = self.paths[random.randint(0, len(self.paths))-1]
            fpath = '{}/{}'.format(path, str(uuid.uuid4())[:-30])
            extrnd = random.randint(1, 20)
            if extrnd not in [self.no_ext, self.bad_ext]:
                csvpath = '{}.{}'.format(fpath, 'csv')
                jsonpath = '{}.{}'.format(fpath, 'json')
                parquetpath = '{}.{}'.format(fpath, 'parquet')
            elif extrnd == self.no_ext: # no extensions
                csvpath = '{}'.format(fpath)
                jsonpath = '{}'.format(fpath)
                parquetpath = '{}'.format(fpath)
            elif extrnd == self.bad_ext: # bad extensions
                csvpath = '{}.{}'.format(fpath, str(uuid.uuid4())[:-30])
                jsonpath = '{}.{}'.format(fpath, str(uuid.uuid4())[:-30])
                parquetpath = '{}.{}'.format(fpath, str(uuid.uuid4())[:-30])
            if lateschema:
                print("lateschema: {}".format(jsonpath))
            if largerec:
                print("largerec: {}".format(parquetpath))
            if malformed:
                print("malformed: {}".format(jsonpath))
            if emptyfile:
                print("emptyfile: {}".format(csvpath))
            if lFile:
                print("largeFile: {}".format(csvpath))
                print("largeFile: {}".format(jsonpath))
                print("largeFile: {}".format(parquetpath))
            data_arrays = [pa.array(dfdata[cind]) for cind in range(num_cols)]
            fields = [pa.field(col, pa.string()) for col in hdr]
            table = pa.Table.from_arrays(data_arrays, schema=pa.schema(fields))
            pa.parquet.write_table(table, parquetpath)
            cdata = '' if emptyfile else '\n'.join(csvdata)
            jdata = '\n'.join(jsondata)
            with open(csvpath, 'w') as cpath:
                cpath.write(cdata + '\n')
            with open(jsonpath, 'w') as jpath:
                jpath.write(jdata + '\n')

    def gen_files(self):
        for ii in range(self.numSchemas):
            self.gen_schema_files()


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-n', '--numSchemas', type=int, action="store", dest="numSchemas", default=20, help="Approx Num Schemas")
    parser.add_option('-f', '--numFiles', type=int, action="store", dest="numFiles", default=10000, help="Approx Num Files")
    parser.add_option('-m', '--minRows', type=int, action="store", dest="minRows", default=10, help="min rows per file")
    parser.add_option('-M', '--maxRows', type=int, action="store", dest="maxRows", default=20, help="max rows per file")
    parser.add_option('-b', '--baseDir', action="store", dest="baseDir", default='mdmdemo', help="base dir")
    parser.add_option('-d', '--depth', type=int, action="store", dest="depth", default=8, help="depth")
    parser.add_option('-s', '--seed', type=int, action="store", dest="seed", default=10, help="random seed")
    parser.add_option('-l', '--largeFile', action="store_true", dest="largeFile", help="use for large file")

    options, args = parser.parse_args()

mdm = MdmFiles(options.numSchemas, options.numFiles, options.minRows, options.maxRows, options.baseDir, options.depth, options.seed, options.largeFile)
mdm.gen_files()
