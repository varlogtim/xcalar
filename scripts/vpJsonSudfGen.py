# This app is part of the visual parser. It takes in a bunch of offsets of
# json tags and outputs a stream UDF that will be then applied to the dataset
# to extract the relevant tags out as records.
import jsonpath_rw, json

def findJsonPath(keyArray, inp):
    # keyArray must be of the form [("key", characterOffset)]
    sortedArray = sorted(keyArray, key=lambda key_offset_type: key_offset_type[1])
    initialFile = open(inp, "r").read()
    segments = []
    prevIndex = 0

    prevOffset = -1
    collapsedArray = []
    for i in range(len(sortedArray)):
        (keyPartialName, charOffset, type) = sortedArray[i]
        if charOffset == prevOffset:
            # collapse the 2 and concat their types
            (pk, pc, pt) = collapsedArray[-1]
            collapsedArray[-1] = (pk, pc, pt + type)
            continue
        collapsedArray.append(sortedArray[i])
        prevOffset = charOffset

    for (keyPartialName, charOffset, type) in collapsedArray:
        #explode initialFile at the correct places
        if (prevIndex == charOffset):
            continue
        segments.append(initialFile[prevIndex:charOffset])
        prevIndex = charOffset
    segments.append(initialFile[prevIndex:])
    withXcTags = ""
    for idx in range(len(segments)-1):
        withXcTags += segments[idx]
        key, offset, type = collapsedArray[idx]
        if key == "{":
            tagToAppend = '"xctag": {"type": "'+ type + '"}'
            if not segments[idx+1][0] == "}":
                tagToAppend += "," #only add if it was not originally empty
            withXcTags += tagToAppend
        else:
            if type == "partial":
                # This is not allowed
                type = "full"
            tagToAppend = '{"xctag": {"array": true, "type": "' + type + '"}}'
            if not segments[idx+1][0] == "]":
                tagToAppend += ","
            withXcTags += tagToAppend
    withXcTags += segments[-1]
    objects = json.loads(withXcTags)
    jsonPath = jsonpath_rw.parse("$..xctag")
    allObj = []
    if isinstance(objects, list):
        for obj in objects:
            allObj += jsonPath.find(obj)
    else:
        allObj += jsonPath.find(objects)
    fullPaths = []
    partialPaths = []
    for obj in allObj:
        isArray = "array" in obj.value
        type = obj.value["type"]
        if isinstance(obj.full_path, jsonpath_rw.jsonpath.Fields):
            # special case for top level
            fullPaths.append("$")
            continue
        path = obj.full_path.left
        if (isArray):
            path = path.left
        origPath = path
        fragments = []
        if type.find("partial") > -1:
            if isinstance(origPath, jsonpath_rw.jsonpath.Fields):
                partialPaths.append("$..'" + str(origPath) + "'")
            elif isinstance(origPath.right, jsonpath_rw.jsonpath.Index):
                # This must be changed to a fullpath
                type = "full"
            else:
                partialPaths.append("$..'" + str(origPath.right) + "'")
        last = True
        if type.find("full") > -1:
            while hasattr(path, "left"):
                if not isinstance(path.right, jsonpath_rw.jsonpath.Index):
                    fragments.append("'" + str(path.right) + "'" )
                else:
                    if last:
                        fragments.append("[" + str(path.right.index) + "]")
                    else:
                        fragments.append("[*]")
                path = path.left
                last = False
            fragments.append("'" + str(path) + "'")
            fullPaths.append("$." + ".".join(reversed(fragments)))
    return (fullPaths, partialPaths)

def constructRecords(keyArray, prettyIn):
    (fullPaths, partialPaths) = findJsonPath(keyArray, prettyIn)
    return """
import json, jsonpath_rw
def parser(inp, ins):
    # Find all partials
    try:
        roots = json.load(ins)
    except:
        ins.seek(0)
        roots = json.loads("[" + ",".join(ins.read().split("\\n")) + "]")
    if (not isinstance(roots, list)):
        roots = [roots]
    partialPaths = """ + json.dumps(partialPaths) + """
    fullPaths = """ + json.dumps(fullPaths) + """
    error = {} # Stores all errors
    parsedPartialPaths = [(p, jsonpath_rw.parse(p)) for p in partialPaths]
    parsedFullPaths = [(p, jsonpath_rw.parse(p)) for p in fullPaths]

    for root in roots:
        for (strPath, partialPath) in parsedPartialPaths:
            element = partialPath.find(root)
            if not isinstance(element[0].value, list):
                element[0].value = [element[0].value]
            for ele in element[0].value:
                path = str(element[0].full_path)
                if (isinstance(ele, dict)):
                    ele["xcPath"] = path
                    ele["xcMethod"] = "partial"
                    yield ele
                else:
                    if strPath in error:
                        error[strPath] += 1
                    else:
                        error[strPath] = 1
        for (strPath, fullPath) in parsedFullPaths:
            element = fullPath.find(root)
            if (not isinstance(element[0].value, list)):
                element[0].value = [element[0].value]

            for ele in element[0].value:
                path = str(element[0].full_path)
                if (isinstance(ele, dict)):
                    ele["xcPath"] = path
                    ele["xcMethod"] = "full"
                    yield ele
                else:
                    if strPath in error:
                        error[strPath] += 1
                    else:
                        error[strPath] = 1
    for e in error:
        yield {"xcParserError": True, "path": e, "numErrors": error[e]}
"""

def adjust(array):
    adjustedArray = []
    for entry in array:
        key = entry["key"]
        offset = entry["offset"]
        type = entry["type"]
        nkey = key.strip()
        adjustedArray.append((nkey, offset, type))
    return adjustedArray

def main(inBlob):
    args = json.loads(inBlob)
    adjustedArray = adjust(args["keys"])
    udf = constructRecords(adjustedArray, args["prettyPath"])
    return json.dumps({"udf": udf})
#print main('{"prettyPath":"/tmp/vp-8932003d80b739f9e2adc861e1dd2a6f", '
#           '"keys":[{"key":"{","type":"full","offset":204},{"key":"{",
# "type":"partial","offset":204},{"key":"{","type":"full","offset":4388},{"key":"{","type":"full","offset":9276}]}')
