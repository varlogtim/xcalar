import json
import optparse
import sys
import traceback

def normJson(normData, norm):
    try:
        for key in norm:
            try:
                normData[key] = normJson({}, json.loads(norm[key]))
            except (TypeError, JSONDecodeError) as e:
                #print(type(e).__name__)
                normData[key] = norm[key]
        return normData
    except:
        return norm

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-j', '--jsonFile', action="store", dest="jsonf", help="json file")
    options, args = parser.parse_args()
    if not options.jsonf:
        print(parser.print_help())
        sys.exit(1)

    with open(options.jsonf) as md:
        norm = json.loads(md.read())

    normData = {}
    normJson(normData, norm)
    print(json.dumps(normData, indent=2))
