import sys, json, re, random, hashlib
from lxml import etree as ET

def getMeta(s, ssf, isArray):
    for k in s:
        if isArray:
            value = k
        else:
            value = s[k]
        if isinstance(value, str) or isinstance(value, str):
            if isArray:
                if "String" not in ssf:
                    ssf["String"] = True
            else:
                if k not in ssf:
                    ssf[k] = {}
                ssf[k]["String"] = True
        elif isinstance(value, bool):
            if isArray:
                if "Boolean" not in ssf:
                    ssf["Boolean"] = True
            else:
                if k not in ssf:
                    ssf[k] = {}
                ssf[k]["Boolean"] = True
        elif isinstance(value, float):
            if isArray:
                if "Float" not in ssf:
                    ssf["Float"] = True
            else:
                if k not in ssf:
                    ssf[k] = {}
                ssf[k]["Float"] = True
        elif isinstance(value, int):
            if isArray:
                if "Integer" not in ssf:
                    ssf["Integer"] = True
            else:
                if k not in ssf:
                    ssf[k] = {}
                ssf[k]["Integer"] = True
        elif isinstance(value, dict):
            if isArray:
                if "Object" not in ssf:
                    ssf["Object"] = {}
                getMeta(value, ssf["Object"], False)
            else:
                if k not in ssf:
                    ssf[k] = {}
                if "Object" not in ssf[k]:
                    ssf[k]["Object"] = {}
                getMeta(value, ssf[k]["Object"], False)
        elif isinstance(value, list):
            if isArray:
                if "Array" not in ssf:
                    ssf["Array"] = {}
                getMeta(value, ssf["Array"], True)
            else:
                if k not in ssf:
                    ssf[k] = {}
                if "Array" not in ssf[k]:
                    ssf[k]["Array"] = {}
                getMeta(value, ssf[k]["Array"], True)
        else:
            # error
            print(value)
            print(type(value))

def getMetaWrapper(s):
    d = {}
    if isinstance(s, list):
        getMeta(s, d, True)
        d = [d]
    else:
        getMeta(s, d, False)
    return d

def reformatMeta(s, indent, step, array):
    out = ""
    for ss in s:
        # must be a key name
        if not array and len(s[ss]) == 1:
            if list(s[ss])[0] != "Object" and list(s[ss])[0] != "Array":
                out += " " * indent + json.dumps(ss) + ": <" + list(s[ss])[0]\
                       + ">,\n"
            else:
                out += " " * indent + json.dumps(ss) + ": "
                if list(s[ss])[0] == "Object":
                    out += "{\n"
                    out += reformatMeta(s[ss]["Object"], indent + step, step,
                                    False)
                    out += " " * indent + "},\n"
                else:
                    out += "[\n"
                    out += reformatMeta(s[ss]["Array"], indent + step, step,
                                    True)
                    out += " " * indent + "],\n"
        elif not array and len(s[ss]) == 0:
            # This is actually a bug
            out += " " * indent + json.dumps(ss) + ": <Unknown>,\n"
        else:
            if not array:
                out += " " * indent + json.dumps(ss) + ":(\n"
                iterObj = s[ss]
            else:
                iterObj = s
            if "String" in iterObj:
                out += " " * (indent + step) + "<String>,\n"
            if "Integer" in iterObj:
                out += " " * (indent + step) + "<Integer>,\n"
            if "Float" in iterObj:
                out += " " * (indent + step) + "<Float>,\n"
            if "Boolean" in iterObj:
                out += " " * (indent + step) + "<Boolean>,\n"
            if "Object" in iterObj:
                out += " " * (indent + step) + "{\n"
                out += reformatMeta(iterObj["Object"], indent + step, step,
                                    False)
                out += " " * (indent + step) + "},\n"
            if "Array" in iterObj:
                out += " " * (indent + step) + "[\n"
                out += reformatMeta(iterObj["Array"], indent + step, step,
                                    True)
                out += " " * (indent + step) + "],\n"
            if not array:
                out += " " * indent + ")\n"
            else:
                break
    return out

def reformat(struct):
    if isinstance(struct, list):
        return reformatMeta(struct[0]["Object"], 0, 2, False)
    else:
        return reformatMeta(struct, 0, 2, False)

def findLongestLineLength(s):
    maxLen = 0
    curSum = 0
    lineNo = 0
    lineLengths = []
    for line in s.split("\n"):
        if lineNo % 100 == 0:
            lineLengths.append(curSum)
        lineLen = len(line)
        curSum += lineLen + 1
        if lineLen > maxLen:
            maxLen = lineLen
        lineNo += 1
    return (lineNo, maxLen, lineLengths, len(s))

def prettyPrintJson(inp, tmpp, metap):
    try:
        structs = json.load(open(inp, "rb"))
    except:
        structs = json.loads("[" +
                             ",".join(open(inp, "rb").read().split("\n")) +
                             "]")
    prettyString = json.dumps(structs, indent=2)
    fout = open(tmpp, "wb")
    fout.write(prettyString)
    fout.close()
    metaPrettyString = reformat(getMetaWrapper(structs))
    fout = open(metap, "wb")
    fout.write(metaPrettyString)
    fout.close()
    return (findLongestLineLength(prettyString), findLongestLineLength(
            metaPrettyString))

def constructXml(elements, root):
    for e in elements:
        elems = root.findall("./" + e.tag)
        if len(elems) == 0:
            newElem = ET.SubElement(root, e.tag)
        else:
            newElem = elems[0]
        constructXml(e.findall("./"), newElem)

def constructXmlMeta(root):
    prettyRoot = ET.Element(root.tag)
    constructXml(root.findall("./"), prettyRoot)
    return prettyRoot

def prettyPrintXml(inp, tmpp, metap):
    parser = ET.XMLParser(remove_blank_text=True, huge_tree=True)
    try:
        parser.feed(open(inp).read().decode("utf-8", "ignore").encode("utf-8"))
        root = parser.close()
    except:
        parser.feed("<xcRecord>")
        parser.feed(open(inp).read().decode("utf-8", "ignore").encode("utf-8"))
        parser.feed("</xcRecord>")
        root = parser.close()
    prettyString = ET.tostring(root, pretty_print=True)
    fout = open(tmpp, "wb")
    fout.write(prettyString)
    fout.close()
    prettyRoot = constructXmlMeta(root)
    metaPrettyString = ET.tostring(prettyRoot, pretty_print=True)
    fout = open(metap, "wb")
    fout.write(metaPrettyString)
    fout.close()
    return (findLongestLineLength(prettyString), findLongestLineLength(
            metaPrettyString))

def prettyPrintText(inp):
    return findLongestLineLength(open(inp, "rb").read())

def main(inBlob):
    arguments = json.loads(inBlob)
    userName = arguments["user"]
    sessionName = arguments["session"]
    hashName = hashlib.md5(userName + "-" + sessionName).hexdigest()
    outPath = "/tmp/vp-" + str(hashName)
    metaOutPath = "/tmp/mvp-" + str(hashName)
    if (arguments["format"] == "xml"):
         ((total, maxLen, lineLengths, numChar),
         (metaTotalLines, metaMaxLen, metaLineLengths,
                                      metaNumChar)) = prettyPrintXml(
                                                      arguments["path"],
                                                      outPath, metaOutPath)
    elif (arguments["format"] == "json"):
        ((total, maxLen, lineLengths, numChar),
         (metaTotalLines, metaMaxLen, metaLineLengths,
                                      metaNumChar)) = prettyPrintJson(
                                                      arguments["path"],
                                                      outPath, metaOutPath)
    elif (arguments["format"] == "text"):
        (total, maxLen, lineLengths, numChar) = prettyPrintText(
                                                arguments["path"])
    if arguments["format"] == "xml" or arguments["format"] == "json":
        metaStruct = {"tmpPath": metaOutPath, "lineLengths": metaLineLengths,
                      "totalLines": metaTotalLines, "maxLen": metaMaxLen,
                      "numChar": metaNumChar}
    else:
        metaStruct = {}
    return json.dumps({"maxLen": maxLen, "lineLengths": lineLengths,
                       "tmpPath": outPath, "totalLines": total,
                       "numChar": numChar,
                       "meta": metaStruct})