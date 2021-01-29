import json
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import (XcalarApi, XcalarApiStatusException)
from xcalar.external.LegacyApi.Operators import Operators


print("X"*80)
print("Xcalar table sizes")
print("X"*80)
c = Client()
print(f"#Xcalar tables = {len(c.list_tables())}")
for tab in c.list_tables():
   #print(f"get_meta: {json.dumps(tab.get_meta(True).as_json_dict(), indent=2)}")
   print(f"Table Name: {tab.get_meta(True).as_json_dict()['attributes']['table_name']}, Table Size : {tab.get_meta(True).total_size_in_bytes}, Table Rows: {tab.get_meta(True).as_json_dict()['aggregated_stats']['total_records_count']}, Incorrect Table Rows: {tab.get_meta().total_records_count}")

print(f"\n{'X'*80}")
print("Published table sizes")
print("X"*80)
xcalarApi = XcalarApi()
operators = Operators(xcalarApi)
listOut = operators.listPublishedTables("*")
print(f"Number of published tables = {len(listOut.tables)}")
for tab in listOut.tables:
    print(f"Table Name {tab.name}, Table Size: {tab.sizeTotal}")
