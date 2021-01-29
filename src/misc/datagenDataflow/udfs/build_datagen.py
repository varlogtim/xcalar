# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

### datagen_lib.py ###

datagen_lib_code = """
import json
import random
import traceback
import logging
import inspect
import copy
from numpy.random import normal
import faker

faker = faker.Faker()

def notdefined():
    return "not defined"

class DataGenX:
    def normal(self, mu, sd, min, max):
        attempt = None
        while (attempt is None or (attempt > max or attempt < min)):
            attempt = normal(mu, sd)
        return attempt
    def wenum(self, **weights):
        r = random.random()
        cur = 0
        for w in weights:
            cur += weights[w]
            if r < cur:
                return w
        return w
    def randallcap(self, min, max):
        slen = faker.random_int(min, max)
        return faker.lexify(text="?" * slen, letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")

datagen =  DataGenX()

class DataGenUtil:
    def seq(self, key):
        self.offsets[key] = self.offsets[key] + 1
        return str(self.offsets[key])

    def __init__(self, numRows, offset, schema):
        self.numRows = numRows
        if numRows <= 0: return
        fields = schema["columns"]
        self.keys = [field["name"] for field in schema["columns"]]
        self.offsets = {key: offset for key in self.keys}
        self.funcs = {key: notdefined for key in self.keys}

        for field in fields:
            key = field["name"]
            ref = field.get("ref",None)
            lf = ""
            try:
                if ref:
                    field = refs[ref]
                ftype = field.get("type", None)
                if not ftype:
                    self.funcs[key] = lambda: "type attribute is reqiured"
                    continue
                fmethod = ftype.split(".")[-1]
                if "." in ftype:
                    fmodule = ftype.split(".")[0]
                    args = field.get("args", {})
                    args = [("{}='{}'" if (type(args[a]).__name__ == 'str') else "{}={}").format(a, args[a]) for a in
                            args]
                    lf = "{}.{}({})".format(fmodule, fmethod, ",".join(args))
                    exec("self.funcs[key] = lambda:" + lf)
                # self.funcs[key] = lambda: fdict.get(field["method"])(**args)
                elif fmethod == "pk":
                    self.funcs[key] = lambda: self.seq(key)
                elif fmethod == "seq":
                    self.offsets[key] = int(field["from"])
                    self.funcs[key] = lambda: self.seq(key)
                elif fmethod == "enum":
                    items = [e.strip() for e in field["args"].split(",")]
                    lf = "{}[random.randint(0, {})]".format(items, len(items) - 1)
                    exec("self.funcs[key] = lambda:" + lf)
                else:
                    exec('self.funcs[key] = lambda:"{} / {} not defined"'.format(ftype, fmethod))
                test = self.funcs[key]()
            except Exception as e:
                exec('self.funcs[key] = lambda:"{}:<{''}> {}'.format(key, lf, traceback.format_exc()))

    def generate(self):
        if self.numRows <= 0: return
        try:
            for i in range(0, self.numRows):
                #TODO: use zip
                record = {key: str(self.funcs[key]()) for key in self.keys}
                yield record
        except Exception as e:
            yield {self.keys[0]: traceback.format_exc()}

def __datagen(partFile, inStream, schema):
    logging.basicConfig(level=logging.INFO)
    inObj = json.loads(inStream.read())
    numRows = inObj["numRows"]
    startRow = inObj["startRow"]
    datagenUtil = DataGenUtil(numRows, startRow, schema)
    yield from datagenUtil.generate()

def getRandInt (fromInt, toInt):
    try:
        return faker.random_int(int(fromInt), int(toInt))
    except Exception as e:
        return str(e)
"""

### all import statements ###

import importlib, io, os, shutil, subprocess, urllib.request, functools, tarfile, traceback, json

### datagen_builder.py ###

class DGDF:
    __counter = 0

    def counter(self):
        self.__counter += 1
        return self.__counter

    def __init__(self, sb):
        # sum(relations,[]) - flattens list
        self.schemas = sb.schemas
        pks = [pk['name'] for pk in sum([f['columns'] for f in self.schemas.values()], []) if pk.get('type',None) == 'pk']
        if (len(set(pks)) != len(pks)):
            # constraint: all pks should be different - otherwise generated table schema would be ugly
            raise Exception("Primary Keys should be unique")
        self.sb = sb
        self.__counter = 0

    def column(self, tname, col, headerName):
        colname = col["name"]
        fpointer = ""
        if not (col.get('type', None) == "pk" or "dtype" in col):
                fpointer = "{}::".format(tname)
        return {
            "columnName": "{}{}".format(fpointer, colname),
            headerName: colname
        }

    def columns(self, source, headerName):
        tname = source.split("#")[0]
        schema = self.schemas[tname]
        columns = schema["columns"]

        relations = [(r, self.schemas[r]["columns"]) for r in schema.get("enrich", {}).keys()]
        fks = [{"name": ("" if x[0] != tname else "relation_") + z["name"],
                "type": z["type"]} for x in relations for z in x[1] if z.get('type', None) == 'pk']

        # sum(relations,[]) - flattens list
        #fks = [c for c in sum(relations, []) if c['type'] == 'pk']
        return [self.column(tname, c, headerName) for c in columns + fks]

    def op_XcalarApiBulkLoad(self, tname, rowcount):
        return {
            "operation": "XcalarApiBulkLoad",
            "args": {
                "dest": ".XcalarDS.xyz.99999.{}".format(tname),
                "loadArgs": {
                    "sourceArgsList": [
                        {
                            "targetName": self.sb.datagen_target,
                            "path": "{}".format(rowcount),
                            "fileNamePattern": "",
                            "recursive": False
                        }
                    ],
                    "parseArgs": {
                        "parserFnName": "{}:datagen_{}".format(self.sb.udf_mudule_name, tname),
                        "parserArgJson": "{}",
                        "fileNameFieldName": "",
                        "recordNumFieldName": "",
                        "allowFileErrors": False,
                        "allowRecordErrors": False,
                        "schema": []
                    },
                    "size": 10737418240
                }
            }
        }

    def op_index_dataset(self, tname):
        dest_table = "{}#{}".format(tname, self.counter())
        print('index', tname, dest_table)
        out = {
            "operation": "XcalarApiIndex",
            "args": {
                "source": ".XcalarDS.xyz.99999.{}".format(tname),
                "dest": dest_table,
                "key": [
                    {
                        "name": "xcalarRecordNum",
                        "keyFieldName": "{}-xcalarRecordNum".format(tname),
                        "type": "DfInt64",
                        "ordering": "Unordered"
                    }
                ],
                "prefix": tname,  # fatpointer
                "dhtName": "",
                "delaySort": False,
                "broadcast": False
            }
        }
        return (dest_table, out)

    def op_cast(self, source):
        tname = source.split("#")[0]
        columns = self.schemas[tname]["columns"]
        dest_table = "{}#{}".format(tname, self.counter())
        # set of referenced table names
        inttset = [c["name"] for c in columns if c.get("dtype") == "int" or c.get("type") == "pk"]
        if len(inttset) > 0:
            print('op_cast', source, dest_table)
            out = {
                "operation": "XcalarApiMap",
                "args": {
                    "source": source,
                    "dest": dest_table,
                    "eval": [{"evalString": "int({}::{}, 10)".format(tname,cname), "newField": cname} for cname in inttset]
                }
            }
            return (dest_table, out)
        else:
            return (source, None)

    def op_max_pk(self, source):
        tname = source.split("#")[0]
        columns = self.schemas[tname]["columns"]
        pk = [c["name"] for c in columns if c.get('type', None) == "pk"]
        if len(pk) == 0:
            return(None, None)
        if len(pk) > 1:
            raise ("There can be only in PK in table {}, found {}".format(tname, len(pk)))
        dest_table = "max_{}_{}".format(tname, pk[0])
        print('op_max_pk', source, dest_table)
        out = {
            "operation": "XcalarApiAggregate",
            "args": {
                "source": source,
                "dest": dest_table,
                "eval": [
                    {
                        "evalString": "max({})".format(pk[0])
                    }
                ]
            }
        }
        return (dest_table, out)

    def op_fks(self, source):
        def fk_map(pk_table):
            columns = self.schemas[pk_table]["columns"]
            #for each referenced table find a primary key
            print(source, '=>', pk_table)
            fk = [c["name"] for c in columns if c.get('type', None) == "pk"]
            if len(fk) == 0:
                raise Exception ("PK for table {} not found".format(pk_table))
            fk = fk[0]
            return {
                    "evalString": "datagen:getRandInt(1, ^max_{}_{})".format(pk_table, fk),
                    "newField": fk if pk_table != tname else "relation_" + fk
                 }
        tname = source.split("#")[0]
        dest_table = "{}#{}".format(tname, self.counter())
        # set of referenced table names
        enrichset = set(self.schemas[tname].get("enrich",{}).keys())
        if len(enrichset) > 0:
            print('op_fks', source, dest_table)
            out = {
                "operation": "XcalarApiMap",
                "args": {
                    "source": source,
                    "dest": dest_table,
                    "eval": [fk_map(rname) for rname in enrichset]
                }
            }
            return (dest_table, out)
        else:
            return(source, None) #TODO: should return none?

    def op_export(self, source):
        tname = source.split("#")[0]
        print('op_export', source)
        columns = self.schemas[tname].get("columns")
        out = {
            "operation": "XcalarApiExport",
            "args": {
                "source": source,
                "fileName": "{}.csv".format(tname),  #
                "targetName": self.sb.export_target,  #
                "targetType": "file",
                "dest": ".XcalarLRQExport.{}".format(source),
                "columns": self.columns(tname, "headerName"),
                "splitRule": "none",   # "size"
                "splitSize": 139959293997168,   # 10000
                "splitNumFiles": 139959293997168,
                "headerType": "every",
                "createRule": "deleteAndReplace",  # overwrite!!! was:   createOnly
                "sorted": True,
                "format": "csv",
                "fieldDelim": "\t",
                "recordDelim": "\n",
                "quoteDelim": "\""
            }
        }
        return (source, out)

    def query(self, schemas):
        # basic datagen load for each table
        out = [self.op_XcalarApiBulkLoad(x, 20) for x in schemas.keys()]
        # index each table
        index_ops = [self.op_index_dataset(x) for x in schemas.keys()]
        out += [o[1] for o in index_ops]

        cast_ops = [self.op_cast(x[0]) for x in index_ops]
        out += [o[1] for o in cast_ops if not o[1] is None]

        # build max aggr for each pk
        pk_ops = [self.op_max_pk(x[0]) for x in cast_ops]
        out += [o[1] for o in pk_ops if not o[1] is None]

        # for each table add fk columns, return same column if none
        fk_ops = [self.op_fks(x[0]) for x in cast_ops]
        out += [o[1] for o in fk_ops if not o[1] is None]

        export_ops = [self.op_export(x[0]) for x in fk_ops]
        out += [o[1] for o in export_ops]

        return ([o[0] for o in export_ops], out)

    def retina(self):
        query = self.query(self.schemas)
        return {
            "tables": [{"name": x, "columns": self.columns(x, "headerAlias")} for x in query[0]],
            "udfs": [
                {
                    "moduleName": self.sb.udf_mudule_name,
                    "fileName": "udfs/{}.py".format(self.sb.udf_mudule_name),
                    "udfType": "python"
                }
            ],
            "query": query[1],
            "xcalarVersion": "1.3.2-1821-xcalardev-7e6cec02-f9fdabe2",
            "dataflowVersion": 1
        }
    
### packager.py ###
    
def assemble(sb, datagen_lib_code):

    root_dir = "/tmp"
    dgdf = DGDF(sb)

    os.chdir(root_dir)
    if not os.path.exists("udfs"):
        os.makedirs("udfs")

    # save json file
    fl = open(os.path.join(root_dir, "dataflowInfo.json"), 'w')
    fl.write(json.dumps(dgdf.retina(), indent=2))
    fl.close()
    schemas =dgdf.schemas
    # save datagen file

    fp = open(os.path.join(root_dir, "udfs", "datagen.py"), 'w')
    fp.write(datagen_lib_code)
    fp.write('\nrefs = ' + json.dumps(sb.refs, indent=2) + '\n')
    fp.write('\ndschemas = ' + json.dumps(schemas, indent=2) + '\n')
    for x in schemas.keys():
        fp.write("def datagen_{}(partFile, inStream):\n".format(x))
        fp.write('   yield from __datagen(partFile, inStream, dschemas["{}"])\n'.format(x))
    fp.close()

    #create archive
    dataFlowFileName = "{}.tar.gz".format(sb.title)
    archive = tarfile.open(dataFlowFileName, "w|gz")
    archive.add("dataflowInfo.json", arcname="dataflowInfo.json")
    archive.add("udfs/datagen.py", arcname="udfs/datagen.py")
    # archive.addfile(tarfile.TarInfo("udfs/datagen.py"), open(os.path.join(root_dir,"datagen.py")))
    archive.close()
    return dataFlowFileName

### Import UDF Datagen.ipynb ###


def getConfigDict():
    from xcalar.compute.api.Env import XcalarConfigPath
    localExportDir = XcalarConfigPath
    cfgData = None
    with open(XcalarConfigPath, 'r') as f:
        cfgData = f.read()
    configdict = {}
    for line in cfgData.splitlines():
        if "=" in line:
            name, var = line.partition("=")[::2]
            configdict[name] = var.strip()
    return configdict
def execute(fullPath, inStream):
    print("&&&&")
    from xcalar.compute.api.XcalarApi import XcalarApi, XcalarApiStatusException
    from xcalar.compute.api.Session import Session
    import xcalar.compute.api.Target2 as target
    import xcalar.compute.api.Target as target0 
    from xcalar.compute.api.Retina import Retina
    from xcalar.compute.api.Operators import Operators 
    import codecs
    from ast import literal_eval
    print("start")
    class SandboxBuilder(object):
        def setup(cls):
            cls.xcalarApi = XcalarApi()
            cls.target = target.Target2(cls.xcalarApi)
            cls.target0 = target0.Target(cls.xcalarApi)
            cls.session = Session(cls.xcalarApi, "BuildSanbox")
            cls.xcalarApi.setSession(cls.session)
            cls.session.activate()
            xcalarConfig = getConfigDict()
            cls.XcalarRootPath = xcalarConfig["Constants.XcalarRootCompletePath"]
            cls.dataflow = Retina(cls.xcalarApi)
            cls.operators = Operators(cls.xcalarApi)
            
        def teardown(cls):
            cls.xcalarApi.setSession(None)
            cls.workbook = None
            cls.xcalarApi = None
    
    sb = SandboxBuilder()
    sb.setup()
# 1) load & validate dataflow json
# 2) set up targets
# 3) build dataflow
# 4) load dataflow
# 5) execute dataflow

# 1) load & validate dataflow json
    step = {"action":"1.load config", "status":"success"}
    try:
        Utf8Reader = codecs.getreader("utf-8")
        utf8Stream = Utf8Reader(inStream)
        execConfig = literal_eval(utf8Stream.read())
        header = execConfig["header"]
        sb.schemas = execConfig["schemas"]
        sb.refs = execConfig.get("reftypes",{})
        sb.title = header["title"]
        sb.udf_mudule_name = header["udf_mudule_name"]
        sb.export_target = header["export_target"]
        sb.datagen_target = header["datagen_target"]
    #    print(execConfig)
    except Exception as e:
        step["status"] = "error"
        step["error"] = format(traceback.format_exc().split('\n'))
    yield step

# 2) set up targets
    step = {"action":"2a.setup datagen target", "status":"success"}
    try:
        datagen = sb.target.add("memory", sb.datagen_target, {})
    except Exception as e:
        step["status"] = "error"
        step["error"] = str(e)
    yield step

    step = {"action":"2b.setup export target folder", "status":"success"}
    try:
        localExportUrl = os.path.join(sb.XcalarRootPath, "export")
        targetFolder = os.path.join(localExportUrl, sb.export_target)
        step["comments"] = makedir(targetFolder)
    except Exception as e:
        step["status"] = "error"
        step["error"] = str(e)
    yield step
    
    step = {"action":"2c.setup export target", "status":"success"}
    try:
        sb.target0.removeSF(sb.export_target)
        step["comments"] = "exisitng {} target replaced; ".format(sb.export_target)
    except:
        pass        
    try:
        args = {
            "mountpoint":  sb.export_target
        }        
        sb.target0.addSF(sb.export_target, targetFolder)
        step["comments"] += "{} - created; ".format(sb.export_target)
    except Exception as e:
        step["status"] = "error"
        step["error"] = format(traceback.format_exc().split('\n'))
    yield step
# 3) build dataflow
    step = {"action":"3.build datagen dataflow", "status":"success"}
    try:
        dataFlowFileName = assemble(sb, datagen_lib_code)
    except Exception as e:
        step["status"] = "error"
        step["error"] = format(traceback.format_exc().split('\n'))
    yield step

# 4) load dataflow
    step = {"action":"4.upload & execute dataflow", "status":"success"}
    try:
        run_retina(sb, dataFlowFileName)
    except Exception as e:
        step["status"] = "error"
        step["error"] = format(traceback.format_exc().split('\n'))
    yield step

    sb.teardown()
def run_retina(sb, dataFlowFileName):
    dataflowContents = None
    try:
        sb.dataflow.delete(sb.title)
    except:
        pass
    with open(dataFlowFileName, "rb") as fp:
        dataflowContents = fp.read()
    sb.dataflow.add(sb.title, dataflowContents)
    sb.dataflow.execute(sb.title,[])
    #TODO: get actual retina execution error
    
def remove(path):
    """ param <path> could either be relative or absolute. """
    try:
        if os.path.isfile(path):
            os.remove(path)  # remove the file
        elif os.path.isdir(path):
            shutil.rmtree(path)  # remove dir and all contains
        else:
            raise ValueError("file {} is not a file or dir.".format(path))
    except Exception as e:
        return str(e)
def makedir(dir):
    #creats directory and all sub-directories
    try:
        os.makedirs(dir, exist_ok=False)
    except Exception as e:
        if e.errno == 17: #if directory already exists - proceeed
            return "ignore: folder {} already exists\n".format(dir)
        else:
            raise (e)
def cpfolder(sourceFolder, destFolder):
    p = subprocess.Popen(['cp', '-r', sourceFolder, destFolder], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    log = 'files copied to ' + destFolder + '\n\n'
    return '\n'.join([str(s).replace('\\n','\n') for s in [log, out, err]])


