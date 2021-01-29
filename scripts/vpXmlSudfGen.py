# This app is part of the visual parser. It takes in a bunch of offsets of
# xml tags and outputs a stream UDF that will be then applied to the dataset
# to extract the relevant tags out as records.
import sys, json, re, xmltodict
from lxml import etree as ET

def findFullXmlPath(keyArray, inp):
    #keyArray must be of the form [("key", characterOffset)]
    sortedArray = sorted(keyArray, key=lambda key_offset: key_offset[1])
    initialFile = open(inp, "r").read()
    segments = []
    prevIndex = 0
    for (keyPartialName, charOffset) in sortedArray:
        #explode initialFile at the correct places
        segments.append(initialFile[prevIndex:charOffset])
        prevIndex = charOffset
    segments.append(initialFile[prevIndex:])
    withXcTags = ""
    for idx in range(len(segments)-1):
        withXcTags += segments[idx]
        withXcTags += "<xctag></xctag>"
        #print str(idx) + ": >>" + segments[idx][-20:] + "<xctag></xctag>" + segments[idx + 1][:20]
    withXcTags += segments[-1]
    root = ET.fromstring(withXcTags)
    allObj = root.findall(".//xctag")
    paths = []
    tree = ET.ElementTree(root)
    for obj in allObj:
        path = tree.getpath(obj.getparent())
        path = re.sub(r"[\[0-9+\]]", "", path)
        paths.append(path)
    return paths

def constructRecords(keyArray, prettyIn):
    # keyArray must be of the form [(key, characterOffset, type)]
    # where type == "full" or "partial"
    fullKeyArray = []
    partialPaths = []
    fullPaths = []
    partialElements = []
    fullElements = []
    for k, o, t in keyArray:
        if t == "full":
            fullKeyArray.append((k, o))
        else:
            partialPaths.append(k)
    fullPaths = findFullXmlPath(fullKeyArray, prettyIn)
    return """
import sys, json, re
from lxml import etree as ET
import xmltodict
def parser(inp, ins):
    # Find all partials
    parser = ET.XMLParser(huge_tree=True)
    try:
        parser.feed(ins.read().decode("utf-8", "ignore").encode("utf-8"))
        root = parser.close()
    except:
        parser = ET.XMLParser(huge_tree=True)
        parser.feed("<xcRecord>")
        parser.feed(open(inp).read().decode("utf-8", "ignore").encode("utf-8"))
        parser.feed("</xcRecord>")
        root = parser.close()
    tree = ET.ElementTree(root)
    tree = ET.ElementTree(root)

    partialPaths = """ + json.dumps(partialPaths) + """
    fullPaths = """ + json.dumps(fullPaths) + """

    if len(partialPaths):
        eString = ".//*[self::" + " or self::".join(partialPaths) + "]"
        print eString
        partialElements = root.xpath(eString)
        for element in partialElements:
            elementDict = xmltodict.parse(ET.tostring(element))
            elementDict["xcPath"] = tree.getpath(element)
            elementDict["xcMethod"] = "partial"
            yield elementDict

    for fullPath in fullPaths:
        fullElements = root.xpath(fullPath)
        for element in fullElements:
            elementDict = xmltodict.parse(ET.tostring(element))
            elementDict["xcPath"] = tree.getpath(element)
            elementDict["xcMethod"] = "full"
            yield elementDict
"""

def adjust(array):
    adjustedArray = []
    for entry in array:
        key = entry["key"]
        offset = entry["offset"]
        type = entry["type"]
        nkey = key.strip()[1:-1]
        if nkey[0] == "/":
            # this is a closing tag. Set offset to be 1 char before <
            offset = offset - len(key)
            nkey = nkey[1:]
        adjustedArray.append((nkey, offset, type))
    return adjustedArray

def main(inBlob):
    args = json.loads(inBlob)
    adjustedArray = adjust(args["keys"])
    udf = constructRecords(adjustedArray, args["prettyPath"])
    return json.dumps({"udf": udf})