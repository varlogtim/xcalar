import sys
import json

retina_data = open(sys.argv[1], 'r').read()
retina_json = json.loads(retina_data)['retina']
retina_obj = json.loads(retina_json)

print(f"QUERY START:\n{retina_obj['query']}\nQUERY END")
print(f"TABLES START:\n{retina_obj['tables']}\nTABLES END")
