import pytest

testlist = [
  {
    "name": "test_0",
    "sample_file": "/saas-load-test/Error_Handling_Test/schema_detection/schema_detection.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/schema_detection/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 10,
    "file_name_pattern": "*.csv",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "test_type": "schema",
    "schema": {
      "rowpath": "$",
      "columns": [
        {
          "name": "ROW_NUMBER",
          "mapping": "$.\"Row_Number\"",
          "type": "DfInt64"
        },
        {
          "name": "CITY_NAME",
          "mapping": "$.\"City_Name\"",
          "type": "DfString"
        },
        {
          "name": "CITY_CANONICAL_NAME",
          "mapping": "$.\"City_Canonical_Name\"",
          "type": "DfString"
        },
        {
          "name": "LAT_LON",
          "mapping": "$.\"Lat_Lon\"",
          "type": "DfString"
        },
        {
          "name": "START_TIME",
          "mapping": "$.\"Start_Time\"",
          "type": "DfInt64"
        },
        {
          "name": "END_TIME",
          "mapping": "$.\"End_Time\"",
          "type": "DfInt64"
        },
        {
          "name": "TEMPERATURE",
          "mapping": "$.\"Temperature\"",
          "type": "DfFloat64"
        },
        {
          "name": "PRESSURE",
          "mapping": "$.\"Pressure\"",
          "type": "DfFloat64"
        },
        {
          "name": "HUMIDITY",
          "mapping": "$.\"Humidity\"",
          "type": "DfInt64"
        },
        {
          "name": "WEATHER_ID",
          "mapping": "$.\"Weather_ID\"",
          "type": "DfInt64"
        }
      ]
    }
  },
  {
    "name": "test_01",
    "sample_file": "/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 10,
    "file_name_pattern": "*.csv",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "test_type": "load",
    "load": [
      {
        "ROW_NUMBER": 117777,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 272.93,
        "PRESSURE": 974.29,
        "HUMIDITY": 98,
        "WEATHER_ID": 501,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117778,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 265.88,
        "PRESSURE": 976.83,
        "HUMIDITY": 94,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117779,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 260.83,
        "PRESSURE": 981.48,
        "HUMIDITY": 69,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117780,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 258.17,
        "PRESSURE": 986.46,
        "HUMIDITY": 63,
        "WEATHER_ID": 600,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117781,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 273.53,
        "PRESSURE": 960.51,
        "HUMIDITY": 100,
        "WEATHER_ID": 501,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117782,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 267.15,
        "PRESSURE": 962.24,
        "HUMIDITY": 87,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117783,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 263.06,
        "PRESSURE": 965.59,
        "HUMIDITY": 68,
        "WEATHER_ID": 601,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117784,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 260.32,
        "PRESSURE": 969.68,
        "HUMIDITY": 57,
        "WEATHER_ID": 600,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117785,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 273.53,
        "PRESSURE": 960.51,
        "HUMIDITY": 100,
        "WEATHER_ID": 501,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117786,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 267.15,
        "PRESSURE": 962.24,
        "HUMIDITY": 87,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117787,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 263.06,
        "PRESSURE": 965.59,
        "HUMIDITY": 68,
        "WEATHER_ID": 601,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117788,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 260.32,
        "PRESSURE": 969.68,
        "HUMIDITY": 57,
        "WEATHER_ID": 600,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      },
      {
        "ROW_NUMBER": 117789,
        "CITY_NAME": "01005",
        "CITY_CANONICAL_NAME": "01005,Massachusetts,United States",
        "LAT_LON": "42.4159,-72.1068",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 273.32,
        "PRESSURE": 960.51,
        "HUMIDITY": 100,
        "WEATHER_ID": 501,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv"
      }
    ],
    "data": [
      {
        "ROW_NUMBER": 117784,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 260.32,
        "PRESSURE": 969.68,
        "HUMIDITY": 57,
        "WEATHER_ID": 600,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 8
      },
      {
        "ROW_NUMBER": 117788,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 260.32,
        "PRESSURE": 969.68,
        "HUMIDITY": 57,
        "WEATHER_ID": 600,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 12
      },
      {
        "ROW_NUMBER": 117780,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 258.17,
        "PRESSURE": 986.46,
        "HUMIDITY": 63,
        "WEATHER_ID": 600,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 4
      },
      {
        "ROW_NUMBER": 117785,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 273.53,
        "PRESSURE": 960.51,
        "HUMIDITY": 100,
        "WEATHER_ID": 501,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 9
      },
      {
        "ROW_NUMBER": 117783,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 263.06,
        "PRESSURE": 965.59,
        "HUMIDITY": 68,
        "WEATHER_ID": 601,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 7
      },
      {
        "ROW_NUMBER": 117778,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 265.88,
        "PRESSURE": 976.83,
        "HUMIDITY": 94,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 2
      },
      {
        "ROW_NUMBER": 117777,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 272.93,
        "PRESSURE": 974.29,
        "HUMIDITY": 98,
        "WEATHER_ID": 501,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 1
      },
      {
        "ROW_NUMBER": 117787,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 263.06,
        "PRESSURE": 965.59,
        "HUMIDITY": 68,
        "WEATHER_ID": 601,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 11
      },
      {
        "ROW_NUMBER": 117779,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 260.83,
        "PRESSURE": 981.48,
        "HUMIDITY": 69,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 3
      },
      {
        "ROW_NUMBER": 117789,
        "CITY_NAME": "01005",
        "CITY_CANONICAL_NAME": "01005,Massachusetts,United States",
        "LAT_LON": "42.4159,-72.1068",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 273.32,
        "PRESSURE": 960.51,
        "HUMIDITY": 100,
        "WEATHER_ID": 501,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 13
      },
      {
        "ROW_NUMBER": 117782,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 267.15,
        "PRESSURE": 962.24,
        "HUMIDITY": 87,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 6
      },
      {
        "ROW_NUMBER": 117786,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 267.15,
        "PRESSURE": 962.24,
        "HUMIDITY": 87,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 10
      },
      {
        "ROW_NUMBER": 117781,
        "CITY_NAME": "01002",
        "CITY_CANONICAL_NAME": "01002,Massachusetts,United States",
        "LAT_LON": "42.37122,-72.49849",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 273.53,
        "PRESSURE": 960.51,
        "HUMIDITY": 100,
        "WEATHER_ID": 501,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Mixed_Schema_CSV/mixed_schema.csv",
        "XcalarRankOver": 5
      }
    ],
    "comp": []
  },
  {
    "name": "test_02",
    "sample_file": "/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 10,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "load": [
      {
        "A": 10,
        "B": 10,
        "C": 9,
        "D": 2,
        "E": 5,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 5,
        "B": 8,
        "C": 6,
        "D": 10,
        "E": 7,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 1,
        "B": 9,
        "C": 1,
        "D": 5,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 5,
        "B": 10,
        "C": 4,
        "D": 8,
        "E": 6,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 1,
        "B": 10,
        "C": 5,
        "D": 9,
        "E": 7,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 1,
        "B": 2,
        "C": 9,
        "D": 6,
        "E": 4,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 5,
        "B": 5,
        "C": 3,
        "D": 4,
        "E": 9,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 9,
        "B": 3,
        "C": 3,
        "D": 7,
        "E": 9,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 3,
        "B": 1,
        "C": 1,
        "D": 10,
        "E": 6,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 9,
        "C": 10,
        "D": 4,
        "E": 6,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 5,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 2,
        "C": 3,
        "D": 2,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 14,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 15,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 1,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 16,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 17,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json"
      }
    ],
    "data": [
      {
        "A": 9,
        "B": 3,
        "C": 3,
        "D": 7,
        "E": 9,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 8
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 5,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 12
      },
      {
        "A": 5,
        "B": 10,
        "C": 4,
        "D": 8,
        "E": 6,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 4
      },
      {
        "A": 7,
        "B": 1,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 16
      },
      {
        "A": 3,
        "B": 1,
        "C": 1,
        "D": 10,
        "E": 6,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 9
      },
      {
        "A": 5,
        "B": 5,
        "C": 3,
        "D": 4,
        "E": 9,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 7
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 17
      },
      {
        "A": 5,
        "B": 8,
        "C": 6,
        "D": 10,
        "E": 7,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 2
      },
      {
        "A": 10,
        "B": 10,
        "C": 9,
        "D": 2,
        "E": 5,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 1
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 11
      },
      {
        "A": 1,
        "B": 9,
        "C": 1,
        "D": 5,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 3
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 13
      },
      {
        "A": 1,
        "B": 2,
        "C": 9,
        "D": 6,
        "E": 4,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 6
      },
      {
        "A": 7,
        "B": 9,
        "C": 10,
        "D": 4,
        "E": 6,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 10
      },
      {
        "A": 7,
        "B": 2,
        "C": 4,
        "D": 2,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 15
      },
      {
        "A": 1,
        "B": 10,
        "C": 5,
        "D": 9,
        "E": 7,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 5
      },
      {
        "A": 7,
        "B": 2,
        "C": 3,
        "D": 2,
        "E": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Middle_Of_File_Schema_JSON/mixed_schema.json",
        "XcalarRankOver": 14
      }
    ],
    "comp": []
  },
  {
    "name": "test_03",
    "sample_file": "/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "load": [
      {
        "ROW_NUMBER": 117777,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 272.93,
        "PRESSURE": 974.29,
        "HUMIDITY": 98,
        "WEATHER_ID": 501,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 117778,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 265.88,
        "PRESSURE": 976.83,
        "HUMIDITY": 94,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 117815,
        "CITY_NAME": "01013",
        "CITY_CANONICAL_NAME": "01013,Massachusetts,United States",
        "LAT_LON": "42.15856,-72.60665",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 260.37,
        "PRESSURE": 981.48,
        "HUMIDITY": 69,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 117816,
        "CITY_NAME": "01013",
        "CITY_CANONICAL_NAME": "01013,Massachusetts,United States",
        "LAT_LON": "42.15856,-72.60665",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 257.73,
        "PRESSURE": 986.46,
        "HUMIDITY": 63,
        "WEATHER_ID": 600,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 117818,
        "CITY_NAME": "01020",
        "CITY_CANONICAL_NAME": "01020,Massachusetts,United States",
        "LAT_LON": "42.17531,-72.57056",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 265.39,
        "PRESSURE": 976.83,
        "HUMIDITY": 94,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 1,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 297.8,
        "PRESSURE": 1020.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 2,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 299.17,
        "PRESSURE": 1023.2,
        "HUMIDITY": 69,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 3,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 300.13,
        "PRESSURE": 1021.92,
        "HUMIDITY": 64,
        "WEATHER_ID": 803,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 4,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 300.98,
        "PRESSURE": 1019.61,
        "HUMIDITY": 60,
        "WEATHER_ID": 804,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 116005,
        "CITY_NAME": "R1N",
        "CITY_CANONICAL_NAME": "R1N,Manitoba,Canada",
        "LAT_LON": "49.97037,-98.30659",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 270.93,
        "PRESSURE": 992.94,
        "HUMIDITY": 64,
        "WEATHER_ID": 801,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      },
      {
        "ROW_NUMBER": 116506,
        "CITY_NAME": "T2A",
        "CITY_CANONICAL_NAME": "T2A,Alberta,Canada",
        "LAT_LON": "51.05193,-113.95568",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 276.66,
        "PRESSURE": 896.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv"
      }
    ],
    "data": [
      {
        "ROW_NUMBER": 3,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 300.13,
        "PRESSURE": 1021.92,
        "HUMIDITY": 64,
        "WEATHER_ID": 803,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 8
      },
      {
        "ROW_NUMBER": 117816,
        "CITY_NAME": "01013",
        "CITY_CANONICAL_NAME": "01013,Massachusetts,United States",
        "LAT_LON": "42.15856,-72.60665",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 257.73,
        "PRESSURE": 986.46,
        "HUMIDITY": 63,
        "WEATHER_ID": 600,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 4
      },
      {
        "ROW_NUMBER": 4,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 300.98,
        "PRESSURE": 1019.61,
        "HUMIDITY": 60,
        "WEATHER_ID": 804,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 9
      },
      {
        "ROW_NUMBER": 2,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 299.17,
        "PRESSURE": 1023.2,
        "HUMIDITY": 69,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 7
      },
      {
        "ROW_NUMBER": 117778,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 265.88,
        "PRESSURE": 976.83,
        "HUMIDITY": 94,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 2
      },
      {
        "ROW_NUMBER": 117777,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 272.93,
        "PRESSURE": 974.29,
        "HUMIDITY": 98,
        "WEATHER_ID": 501,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 1
      },
      {
        "ROW_NUMBER": 116506,
        "CITY_NAME": "T2A",
        "CITY_CANONICAL_NAME": "T2A,Alberta,Canada",
        "LAT_LON": "51.05193,-113.95568",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 276.66,
        "PRESSURE": 896.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 11
      },
      {
        "ROW_NUMBER": 117815,
        "CITY_NAME": "01013",
        "CITY_CANONICAL_NAME": "01013,Massachusetts,United States",
        "LAT_LON": "42.15856,-72.60665",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 260.37,
        "PRESSURE": 981.48,
        "HUMIDITY": 69,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 3
      },
      {
        "ROW_NUMBER": 1,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 297.8,
        "PRESSURE": 1020.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 6
      },
      {
        "ROW_NUMBER": 116005,
        "CITY_NAME": "R1N",
        "CITY_CANONICAL_NAME": "R1N,Manitoba,Canada",
        "LAT_LON": "49.97037,-98.30659",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 270.93,
        "PRESSURE": 992.94,
        "HUMIDITY": 64,
        "WEATHER_ID": 801,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 10
      },
      {
        "ROW_NUMBER": 117818,
        "CITY_NAME": "01020",
        "CITY_CANONICAL_NAME": "01020,Massachusetts,United States",
        "LAT_LON": "42.17531,-72.57056",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 265.39,
        "PRESSURE": 976.83,
        "HUMIDITY": 94,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/ChangingDataType_Line44_CSV/changing_data_type.csv",
        "XcalarRankOver": 5
      }
    ],
    "comp": []
  },
  {
    "name": "test_04",
    "sample_file": "/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
"load" : [
  {
    "A": 10,
    "B": 10,
    "C": 9,
    "D": 2,
    "E": 5,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "A": 5,
    "B": 8,
    "C": 6,
    "D": 10,
    "E": 7,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "A": 1,
    "B": 9,
    "C": 1,
    "D": 5,
    "E": 1,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 3,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "A": 7,
    "B": 9,
    "C": 10,
    "D": 4,
    "E": 6,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 4,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "XCALAR_ICV": "ERROR: A: DfInt64(A) contains non-digit char(0x41) at 0",
    "XCALAR_FILE_RECORD_NUM": 5,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': 'A', 'B': 10, 'C': 1, 'D': 8, 'E': 9}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "XCALAR_ICV": "ERROR: A: DfInt64(DfInt64) contains non-digit char(0x2e) at 1",
    "XCALAR_FILE_RECORD_NUM": 6,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': 2.3, 'B': 2, 'C': 4, 'D': 2, 'E': 1}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "XCALAR_ICV": "ERROR: A: DfInt64(True) contains non-digit char(0x54) at 0",
    "XCALAR_FILE_RECORD_NUM": 7,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': True, 'B': 10, 'C': 10, 'D': 1, 'E': 8}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  },
  {
    "B": 4,
    "C": 10,
    "D": 10,
    "E": 6,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 8,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json"
  }
],
"data" : [
  {
    "A": 7,
    "B": 9,
    "C": 10,
    "D": 4,
    "E": 6,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 4
  },
  {
    "A": 5,
    "B": 8,
    "C": 6,
    "D": 10,
    "E": 7,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 2
  },
  {
    "A": 10,
    "B": 10,
    "C": 9,
    "D": 2,
    "E": 5,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 1
  },
  {
    "A": 1,
    "B": 9,
    "C": 1,
    "D": 5,
    "E": 1,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 3
  },
  {
    "B": 4,
    "C": 10,
    "D": 10,
    "E": 6,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 5
  }
],
"comp" : [
  {
    "XCALAR_ICV": "ERROR: A: DfInt64(DfInt64) contains non-digit char(0x2e) at 1",
    "XCALAR_FILE_RECORD_NUM": 6,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': 2.3, 'B': 2, 'C': 4, 'D': 2, 'E': 1}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 2
  },
  {
    "XCALAR_ICV": "ERROR: A: DfInt64(A) contains non-digit char(0x41) at 0",
    "XCALAR_FILE_RECORD_NUM": 5,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': 'A', 'B': 10, 'C': 1, 'D': 8, 'E': 9}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 1
  },
  {
    "XCALAR_ICV": "ERROR: A: DfInt64(True) contains non-digit char(0x54) at 0",
    "XCALAR_FILE_RECORD_NUM": 7,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': True, 'B': 10, 'C': 10, 'D': 1, 'E': 8}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Changing_DataType_Line11_JSON/changin_data_type.json",
    "XcalarRankOver": 3
  }
]
},
{
    "name": "test_05",
    "sample_file": "/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/Missing_Column_CSV/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "load": [
      {
        "ROW_NUMBER": 117777,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 272.93,
        "PRESSURE": 974.29,
        "HUMIDITY": 98,
        "WEATHER_ID": 501,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv"
      },
      {
        "ROW_NUMBER": 117786,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 267.15,
        "PRESSURE": 962.24,
        "HUMIDITY": 87,
        "WEATHER_ID": 500,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv"
      },
      {
        "ROW_NUMBER": 117787,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 263.06,
        "PRESSURE": 965.59,
        "HUMIDITY": 68,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv"
      },
      {
        "ROW_NUMBER": 117788,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 260.32,
        "PRESSURE": 969.68,
        "HUMIDITY": 57,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv"
      }
    ],
    "data": [
      {
        "ROW_NUMBER": 117788,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 260.32,
        "PRESSURE": 969.68,
        "HUMIDITY": 57,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv",
        "XcalarRankOver": 4
      },
      {
        "ROW_NUMBER": 117786,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 267.15,
        "PRESSURE": 962.24,
        "HUMIDITY": 87,
        "WEATHER_ID": 500,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv",
        "XcalarRankOver": 2
      },
      {
        "ROW_NUMBER": 117777,
        "CITY_NAME": "01001",
        "CITY_CANONICAL_NAME": "01001,Massachusetts,United States",
        "LAT_LON": "42.06592,-72.6216",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 272.93,
        "PRESSURE": 974.29,
        "HUMIDITY": 98,
        "WEATHER_ID": 501,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv",
        "XcalarRankOver": 1
      },
      {
        "ROW_NUMBER": 117787,
        "CITY_NAME": "01003",
        "CITY_CANONICAL_NAME": "01003,Massachusetts,United States",
        "LAT_LON": "42.3911,-72.52523",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 263.06,
        "PRESSURE": 965.59,
        "HUMIDITY": 68,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_CSV/missing_column.csv",
        "XcalarRankOver": 3
      }
    ],
    "comp": []
  },
  {
    "name": "test_06",
    "sample_file": "/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Missing_Column_JSON/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
"load" : [
{
"A": 10,
"B": 10,
"C": 9,
"D": 2,
"E": 5,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 1,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
},
{
"A": 5,
"B": 8,
"C": 6,
"D": 10,
"E": 7,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 2,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
},
{
"A": 1,
"B": 9,
"C": 1,
"D": 5,
"E": 1,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 3,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
},
{
"A": 3,
"B": 1,
"C": 1,
"D": 10,
"E": 6,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 4,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
},
{
"A": 7,
"B": 9,
"C": 10,
"D": 4,
"E": 6,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 5,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
},
{
"A": 7,
"C": 4,
"E": 1,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 6,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
},
{
"A": 7,
"B": 2,
"E": 1,
"XCALAR_ICV": "",
"XCALAR_FILE_RECORD_NUM": 7,
"XCALAR_SOURCEDATA": "",
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json"
}
],
"data" : [
{
"A": 3,
"B": 1,
"C": 1,
"D": 10,
"E": 6,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 4
},
{
"A": 7,
"B": 2,
"E": 1,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 7
},
{
"A": 5,
"B": 8,
"C": 6,
"D": 10,
"E": 7,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 2
},
{
"A": 10,
"B": 10,
"C": 9,
"D": 2,
"E": 5,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 1
},
{
"A": 1,
"B": 9,
"C": 1,
"D": 5,
"E": 1,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 3
},
{
"A": 7,
"C": 4,
"E": 1,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 6
},
{
"A": 7,
"B": 9,
"C": 10,
"D": 4,
"E": 6,
"XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Missing_Column_JSON/missing_column.json",
"XcalarRankOver": 5
}
],
"comp" : []
  },
  {
    "name": "test_07",
    "sample_file": "/saas-load-test/Error_Handling_Test/Large_Nested_Json/DB00531.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Large_Nested_Json/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.json",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "AWS ERROR: The character number in one record is more than our max threshold, maxCharsPerRecord: 1,048,576"
    },
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_08",
    "sample_file": "/saas-load-test/Error_Handling_Test/Single_Line_Nested_Json/drugbank_LINES.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Single_Line_Nested_Json/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
  "load": [
    {
      "DRUGBANK_ID_0": "DB11010",
      "ALTERNATE_DRUGBANK_IDS_0": "DB10978",
      "ALTERNATE_DRUGBANK_IDS_1": "DB10524",
      "NAME_0": "Cod, unspecified",
      "DESCRIPTION_0": "Cod, unspecified allergenic extract is used in allergenic testing.",
      "GROUPS_0": "approved",
      "DRUG_TYPE": "biotech",
      "SYNONYM_0": "Cod",
      "LANGUAGE_0": "english",
      "SYNONYM_1": "Cod, unspecified preparation",
      "LANGUAGE_1": "english",
      "SYNONYM_2": "Codfish",
      "LANGUAGE_2": "english",
      "SYNONYM_3": "codfish allergenic extract",
      "LANGUAGE_3": "english",
      "UNII": "8D6Q5LNG3D",
      "OFF_LABEL": False,
      "OTC_USE": False,
      "MILITARY_USE": False,
      "KIND": "used_in_combination_for_diagnostic_process",
      "COMBINATION_TYPE": "mixture",
      "TITLE_0": "Allergenic testing",
      "DRUGBANK_ID_1": "DBCOND0103395",
      "DIRECT_PARENT": "Peptides",
      "KINGDOM": "Organic Compounds",
      "SUPERCLASS_NAME": "Organic Acids",
      "CLASS_NAME": "Carboxylic Acids and Derivatives",
      "SUBCLASS_NAME": "Amino Acids, Peptides, and Analogues",
      "NAME_1": "Codfish",
      "NDC_PRODUCT_CODE_0": "54575-379",
      "STARTED_MARKETING_ON_0": "1972-08-29",
      "DOSAGE_FORM_0": "Injection, solution",
      "STRENGTH_0": "1 g/20mL",
      "ROUTE_0": "Percutaneous; Subcutaneous",
      "FDA_APPLICATION_NUMBER_0": "BLA102192",
      "COUNTRY_0": "US",
      "OVER_THE_COUNTER_0": False,
      "GENERIC_0": False,
      "APPROVED_0": True,
      "SOURCE_0": "FDA NDC",
      "NAME_2": "Allergy Laboratories, Inc.",
      "NAME_3": "Cod, unspecified",
      "DRUGBANK_ID_2": "DB11010",
      "NUMBER_0": "1",
      "UNIT_0": "g/20mL",
      "NAME_4": "Codfish",
      "NDC_PRODUCT_CODE_1": "36987-1202",
      "STARTED_MARKETING_ON_1": "1972-08-29",
      "DOSAGE_FORM_1": "Injection, solution",
      "STRENGTH_1": ".05 g/mL",
      "ROUTE_1": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_1": "BLA102192",
      "COUNTRY_1": "US",
      "OVER_THE_COUNTER_1": False,
      "GENERIC_1": False,
      "APPROVED_1": True,
      "SOURCE_1": "FDA NDC",
      "NAME_5": "Nelco Laboratories, Inc.",
      "NAME_6": "Cod, unspecified",
      "DRUGBANK_ID_3": "DB11010",
      "NUMBER_1": ".05",
      "UNIT_1": "g/mL",
      "NAME_7": "Codfish",
      "NDC_PRODUCT_CODE_2": "36987-1203",
      "STARTED_MARKETING_ON_2": "1972-08-29",
      "DOSAGE_FORM_2": "Injection, solution",
      "STRENGTH_2": ".05 g/mL",
      "ROUTE_2": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_2": "BLA102192",
      "COUNTRY_2": "US",
      "OVER_THE_COUNTER_2": False,
      "GENERIC_2": False,
      "APPROVED_2": True,
      "SOURCE_2": "FDA NDC",
      "NAME_8": "Nelco Laboratories, Inc.",
      "NAME_9": "Cod, unspecified",
      "DRUGBANK_ID_4": "DB11010",
      "NUMBER_2": ".05",
      "UNIT_2": "g/mL",
      "NAME_10": "Codfish",
      "NDC_PRODUCT_CODE_3": "36987-1204",
      "STARTED_MARKETING_ON_3": "1972-08-29",
      "DOSAGE_FORM_3": "Injection, solution",
      "STRENGTH_3": ".1 g/mL",
      "ROUTE_3": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_3": "BLA102192",
      "COUNTRY_3": "US",
      "OVER_THE_COUNTER_3": False,
      "GENERIC_3": False,
      "APPROVED_3": True,
      "SOURCE_3": "FDA NDC",
      "NAME_11": "Nelco Laboratories, Inc.",
      "NAME_12": "Cod, unspecified",
      "DRUGBANK_ID_5": "DB11010",
      "NUMBER_3": ".1",
      "UNIT_3": "g/mL",
      "NAME_13": "Codfish",
      "NDC_PRODUCT_CODE_4": "36987-1205",
      "STARTED_MARKETING_ON_4": "1972-08-29",
      "DOSAGE_FORM_4": "Injection, solution",
      "STRENGTH_4": ".1 g/mL",
      "ROUTE_4": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_4": "BLA102192",
      "COUNTRY_4": "US",
      "OVER_THE_COUNTER_4": False,
      "GENERIC_4": False,
      "APPROVED_4": True,
      "SOURCE_4": "FDA NDC",
      "NAME_14": "Nelco Laboratories, Inc.",
      "NAME_15": "Cod, unspecified",
      "DRUGBANK_ID_6": "DB11010",
      "NUMBER_4": ".1",
      "UNIT_4": "g/mL",
      "NAME_16": "Food - Fish and Shellfish, Codfish Gadus callarias",
      "NDC_PRODUCT_CODE_5": "65044-3203",
      "STARTED_MARKETING_ON_5": "1941-04-19",
      "DOSAGE_FORM_5": "Injection, solution",
      "STRENGTH_5": ".1 g/mL",
      "ROUTE_5": "Percutaneous; Subcutaneous",
      "FDA_APPLICATION_NUMBER_5": "BLA103888",
      "COUNTRY_5": "US",
      "OVER_THE_COUNTER_5": False,
      "GENERIC_5": False,
      "APPROVED_5": True,
      "SOURCE_5": "FDA NDC",
      "NAME_17": "Jubilant Hollisterstier Llc",
      "NAME_18": "Cod, unspecified",
      "DRUGBANK_ID_7": "DB11010",
      "NUMBER_5": ".1",
      "UNIT_5": "g/mL",
      "NAME_19": "Food - Fish and Shellfish, Codfish Gadus callarias",
      "NDC_PRODUCT_CODE_6": "65044-3204",
      "STARTED_MARKETING_ON_6": "1941-04-19",
      "DOSAGE_FORM_6": "Injection, solution",
      "STRENGTH_6": ".1 g/mL",
      "ROUTE_6": "Percutaneous",
      "FDA_APPLICATION_NUMBER_6": "BLA103888",
      "COUNTRY_6": "US",
      "OVER_THE_COUNTER_6": False,
      "GENERIC_6": False,
      "APPROVED_6": True,
      "SOURCE_6": "FDA NDC",
      "NAME_20": "Jubilant Hollisterstier Llc",
      "NAME_21": "Cod, unspecified",
      "DRUGBANK_ID_8": "DB11010",
      "NUMBER_6": ".1",
      "UNIT_6": "g/mL",
      "REGION_0": "CA",
      "MAX_PHASE_0": 0,
      "MARKETED_PRESCRIPTION_0": False,
      "GENERIC_AVAILABLE_0": False,
      "PRE_MARKET_CANCELLED_0": False,
      "POST_MARKET_CANCELLED_0": False,
      "REGION_1": "EU",
      "MAX_PHASE_1": 0,
      "MARKETED_PRESCRIPTION_1": False,
      "GENERIC_AVAILABLE_1": False,
      "PRE_MARKET_CANCELLED_1": False,
      "POST_MARKET_CANCELLED_1": False,
      "REGION_2": "US",
      "MAX_PHASE_2": 4,
      "MARKETED_PRESCRIPTION_2": True,
      "GENERIC_AVAILABLE_2": False,
      "PRE_MARKET_CANCELLED_2": False,
      "POST_MARKET_CANCELLED_2": False,
      "TITLE_1": "Non-Standardized Food Allergenic Extract",
      "DRUGBANK_ID_9": "DBCAT003528",
      "CATEGORIZATION_KIND": "therapeutic",
      "FORM_0": "Injection, solution",
      "ROUTE_7": "Intradermal; Subcutaneous",
      "STRENGTH_7": ".05 g/mL",
      "FORM_1": "Injection, solution",
      "ROUTE_8": "Intradermal; Subcutaneous",
      "STRENGTH_8": ".1 g/mL",
      "FORM_2": "Injection, solution",
      "ROUTE_9": "Percutaneous; Subcutaneous",
      "STRENGTH_9": "1 g/20mL",
      "FORM_3": "Injection, solution",
      "ROUTE_10": "Percutaneous",
      "STRENGTH_10": ".1 g/mL",
      "FORM_4": "Injection, solution",
      "ROUTE_11": "Percutaneous; Subcutaneous",
      "STRENGTH_11": ".1 g/mL",
      "RESOURCE": "PubChem Substance",
      "IDENTIFIER": 347911079,
      "XCALAR_ICV": "",
      "XCALAR_FILE_RECORD_NUM": 1,
      "XCALAR_SOURCEDATA": "",
      "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Single_Line_Nested_Json/drugbank_LINES.json"
    }
  ],
  "data": [
    {
      "DRUGBANK_ID_0": "DB11010",
      "ALTERNATE_DRUGBANK_IDS_0": "DB10978",
      "ALTERNATE_DRUGBANK_IDS_1": "DB10524",
      "NAME_0": "Cod, unspecified",
      "DESCRIPTION_0": "Cod, unspecified allergenic extract is used in allergenic testing.",
      "GROUPS_0": "approved",
      "DRUG_TYPE": "biotech",
      "SYNONYM_0": "Cod",
      "LANGUAGE_0": "english",
      "SYNONYM_1": "Cod, unspecified preparation",
      "LANGUAGE_1": "english",
      "SYNONYM_2": "Codfish",
      "LANGUAGE_2": "english",
      "SYNONYM_3": "codfish allergenic extract",
      "LANGUAGE_3": "english",
      "UNII": "8D6Q5LNG3D",
      "OFF_LABEL": False,
      "OTC_USE": False,
      "MILITARY_USE": False,
      "KIND": "used_in_combination_for_diagnostic_process",
      "COMBINATION_TYPE": "mixture",
      "TITLE_0": "Allergenic testing",
      "DRUGBANK_ID_1": "DBCOND0103395",
      "DIRECT_PARENT": "Peptides",
      "KINGDOM": "Organic Compounds",
      "SUPERCLASS_NAME": "Organic Acids",
      "CLASS_NAME": "Carboxylic Acids and Derivatives",
      "SUBCLASS_NAME": "Amino Acids, Peptides, and Analogues",
      "NAME_1": "Codfish",
      "NDC_PRODUCT_CODE_0": "54575-379",
      "STARTED_MARKETING_ON_0": "1972-08-29",
      "DOSAGE_FORM_0": "Injection, solution",
      "STRENGTH_0": "1 g/20mL",
      "ROUTE_0": "Percutaneous; Subcutaneous",
      "FDA_APPLICATION_NUMBER_0": "BLA102192",
      "COUNTRY_0": "US",
      "OVER_THE_COUNTER_0": False,
      "GENERIC_0": False,
      "APPROVED_0": True,
      "SOURCE_0": "FDA NDC",
      "NAME_2": "Allergy Laboratories, Inc.",
      "NAME_3": "Cod, unspecified",
      "DRUGBANK_ID_2": "DB11010",
      "NUMBER_0": "1",
      "UNIT_0": "g/20mL",
      "NAME_4": "Codfish",
      "NDC_PRODUCT_CODE_1": "36987-1202",
      "STARTED_MARKETING_ON_1": "1972-08-29",
      "DOSAGE_FORM_1": "Injection, solution",
      "STRENGTH_1": ".05 g/mL",
      "ROUTE_1": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_1": "BLA102192",
      "COUNTRY_1": "US",
      "OVER_THE_COUNTER_1": False,
      "GENERIC_1": False,
      "APPROVED_1": True,
      "SOURCE_1": "FDA NDC",
      "NAME_5": "Nelco Laboratories, Inc.",
      "NAME_6": "Cod, unspecified",
      "DRUGBANK_ID_3": "DB11010",
      "NUMBER_1": ".05",
      "UNIT_1": "g/mL",
      "NAME_7": "Codfish",
      "NDC_PRODUCT_CODE_2": "36987-1203",
      "STARTED_MARKETING_ON_2": "1972-08-29",
      "DOSAGE_FORM_2": "Injection, solution",
      "STRENGTH_2": ".05 g/mL",
      "ROUTE_2": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_2": "BLA102192",
      "COUNTRY_2": "US",
      "OVER_THE_COUNTER_2": False,
      "GENERIC_2": False,
      "APPROVED_2": True,
      "SOURCE_2": "FDA NDC",
      "NAME_8": "Nelco Laboratories, Inc.",
      "NAME_9": "Cod, unspecified",
      "DRUGBANK_ID_4": "DB11010",
      "NUMBER_2": ".05",
      "UNIT_2": "g/mL",
      "NAME_10": "Codfish",
      "NDC_PRODUCT_CODE_3": "36987-1204",
      "STARTED_MARKETING_ON_3": "1972-08-29",
      "DOSAGE_FORM_3": "Injection, solution",
      "STRENGTH_3": ".1 g/mL",
      "ROUTE_3": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_3": "BLA102192",
      "COUNTRY_3": "US",
      "OVER_THE_COUNTER_3": False,
      "GENERIC_3": False,
      "APPROVED_3": True,
      "SOURCE_3": "FDA NDC",
      "NAME_11": "Nelco Laboratories, Inc.",
      "NAME_12": "Cod, unspecified",
      "DRUGBANK_ID_5": "DB11010",
      "NUMBER_3": ".1",
      "UNIT_3": "g/mL",
      "NAME_13": "Codfish",
      "NDC_PRODUCT_CODE_4": "36987-1205",
      "STARTED_MARKETING_ON_4": "1972-08-29",
      "DOSAGE_FORM_4": "Injection, solution",
      "STRENGTH_4": ".1 g/mL",
      "ROUTE_4": "Intradermal; Subcutaneous",
      "FDA_APPLICATION_NUMBER_4": "BLA102192",
      "COUNTRY_4": "US",
      "OVER_THE_COUNTER_4": False,
      "GENERIC_4": False,
      "APPROVED_4": True,
      "SOURCE_4": "FDA NDC",
      "NAME_14": "Nelco Laboratories, Inc.",
      "NAME_15": "Cod, unspecified",
      "DRUGBANK_ID_6": "DB11010",
      "NUMBER_4": ".1",
      "UNIT_4": "g/mL",
      "NAME_16": "Food - Fish and Shellfish, Codfish Gadus callarias",
      "NDC_PRODUCT_CODE_5": "65044-3203",
      "STARTED_MARKETING_ON_5": "1941-04-19",
      "DOSAGE_FORM_5": "Injection, solution",
      "STRENGTH_5": ".1 g/mL",
      "ROUTE_5": "Percutaneous; Subcutaneous",
      "FDA_APPLICATION_NUMBER_5": "BLA103888",
      "COUNTRY_5": "US",
      "OVER_THE_COUNTER_5": False,
      "GENERIC_5": False,
      "APPROVED_5": True,
      "SOURCE_5": "FDA NDC",
      "NAME_17": "Jubilant Hollisterstier Llc",
      "NAME_18": "Cod, unspecified",
      "DRUGBANK_ID_7": "DB11010",
      "NUMBER_5": ".1",
      "UNIT_5": "g/mL",
      "NAME_19": "Food - Fish and Shellfish, Codfish Gadus callarias",
      "NDC_PRODUCT_CODE_6": "65044-3204",
      "STARTED_MARKETING_ON_6": "1941-04-19",
      "DOSAGE_FORM_6": "Injection, solution",
      "STRENGTH_6": ".1 g/mL",
      "ROUTE_6": "Percutaneous",
      "FDA_APPLICATION_NUMBER_6": "BLA103888",
      "COUNTRY_6": "US",
      "OVER_THE_COUNTER_6": False,
      "GENERIC_6": False,
      "APPROVED_6": True,
      "SOURCE_6": "FDA NDC",
      "NAME_20": "Jubilant Hollisterstier Llc",
      "NAME_21": "Cod, unspecified",
      "DRUGBANK_ID_8": "DB11010",
      "NUMBER_6": ".1",
      "UNIT_6": "g/mL",
      "REGION_0": "CA",
      "MAX_PHASE_0": 0,
      "MARKETED_PRESCRIPTION_0": False,
      "GENERIC_AVAILABLE_0": False,
      "PRE_MARKET_CANCELLED_0": False,
      "POST_MARKET_CANCELLED_0": False,
      "REGION_1": "EU",
      "MAX_PHASE_1": 0,
      "MARKETED_PRESCRIPTION_1": False,
      "GENERIC_AVAILABLE_1": False,
      "PRE_MARKET_CANCELLED_1": False,
      "POST_MARKET_CANCELLED_1": False,
      "REGION_2": "US",
      "MAX_PHASE_2": 4,
      "MARKETED_PRESCRIPTION_2": True,
      "GENERIC_AVAILABLE_2": False,
      "PRE_MARKET_CANCELLED_2": False,
      "POST_MARKET_CANCELLED_2": False,
      "TITLE_1": "Non-Standardized Food Allergenic Extract",
      "DRUGBANK_ID_9": "DBCAT003528",
      "CATEGORIZATION_KIND": "therapeutic",
      "FORM_0": "Injection, solution",
      "ROUTE_7": "Intradermal; Subcutaneous",
      "STRENGTH_7": ".05 g/mL",
      "FORM_1": "Injection, solution",
      "ROUTE_8": "Intradermal; Subcutaneous",
      "STRENGTH_8": ".1 g/mL",
      "FORM_2": "Injection, solution",
      "ROUTE_9": "Percutaneous; Subcutaneous",
      "STRENGTH_9": "1 g/20mL",
      "FORM_3": "Injection, solution",
      "ROUTE_10": "Percutaneous",
      "STRENGTH_10": ".1 g/mL",
      "FORM_4": "Injection, solution",
      "ROUTE_11": "Percutaneous; Subcutaneous",
      "STRENGTH_11": ".1 g/mL",
      "RESOURCE": "PubChem Substance",
      "IDENTIFIER": 347911079,
      "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Single_Line_Nested_Json/drugbank_LINES.json",
      "XcalarRankOver": 1
    }
  ],
  "comp": []
  },
  {
    "name": "test_13",
    "comment": "ENG-9516 - fix not in v1.0",
    "sample_file": "/saas-load-test/Error_Handling_Test/Non_UTF/nonutf.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Non_UTF/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.json",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "AWS ERROR: UTF-8 encoding is required. The text encoding error was found near byte 14."
    },
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_15",
    "comment": "ENG-9791 - An error occurred (LexerInvalidChar) when calling the SelectObjectContent operation",
    "sample_file": "/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/Quoted_Column/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE",
        "QuoteCharacter": '\"'
      }
    },
    "num_rows": 5,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.xfail,
    "load": [
      {
        "ROW_NUMBER": 1,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 297.8,
        "PRESSURE": 1020.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv"
      },
      {
        "ROW_NUMBER": 2,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 299.17,
        "PRESSURE": 1023.2,
        "HUMIDITY": 69,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv"
      },
      {
        "ROW_NUMBER": 3,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 300.13,
        "PRESSURE": 1021.92,
        "HUMIDITY": 64,
        "WEATHER_ID": 803,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv"
      },
      {
        "ROW_NUMBER": 4,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 300.98,
        "PRESSURE": 1019.61,
        "HUMIDITY": 60,
        "WEATHER_ID": 804,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv"
      },
      {
        "ROW_NUMBER": 5,
        "CITY___NAME": "Al Ain",
        "CITY_CANONICAL_NAME": "Al Ain,Abu Dhabi,United Arab Emirates",
        "LAT_LON": "24.20732,55.68615",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 295.15,
        "PRESSURE": 982.89,
        "HUMIDITY": 40,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv"
      }
    ],
    "data": [
      {
        "ROW_NUMBER": 4,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394712000,
        "END_TIME": 1394722799,
        "TEMPERATURE": 300.98,
        "PRESSURE": 1019.61,
        "HUMIDITY": 60,
        "WEATHER_ID": 804,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
        "XcalarRankOver": 4
      },
      {
        "ROW_NUMBER": 2,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 299.17,
        "PRESSURE": 1023.2,
        "HUMIDITY": 69,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
        "XcalarRankOver": 2
      },
      {
        "ROW_NUMBER": 1,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 297.8,
        "PRESSURE": 1020.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
        "XcalarRankOver": 1
      },
      {
        "ROW_NUMBER": 3,
        "CITY___NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 300.13,
        "PRESSURE": 1021.92,
        "HUMIDITY": 64,
        "WEATHER_ID": 803,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
        "XcalarRankOver": 3
      },
      {
        "ROW_NUMBER": 5,
        "CITY___NAME": "Al Ain",
        "CITY_CANONICAL_NAME": "Al Ain,Abu Dhabi,United Arab Emirates",
        "LAT_LON": "24.20732,55.68615",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 295.15,
        "PRESSURE": 982.89,
        "HUMIDITY": 40,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
        "XcalarRankOver": 5
      }
    ],
    "comp": []
  },
  {
    "name": "test_16",
    "sample_file": "/saas-load-test/Error_Handling_Test/2500_Cols/2500_cols0.0.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/2500_Cols/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 5,
    "file_name_pattern": "*.parquet",
    "test_type": "custom",
    "recursive": True,
    "exec": "assert len(resp['rows'][0]) > 1000",
    "mark": pytest.mark.xfail
  },
  {
    "name": "test_22",
    "sample_file": "/saas-load-test/Error_Handling_Test/CSV_NO_Extension/no_extension",
    "dataset": "/saas-load-test/Error_Handling_Test/CSV_NO_Extension/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*",
    "mark": pytest.mark.precheckin,
    "test_type": "schema",
    "recursive": True,
    "schema": {
      "rowpath": "$",
      "columns": [
        {
          "name": "ROW_NUMBER",
          "mapping": "$.\"Row_Number\"",
          "type": "DfInt64"
        },
        {
          "name": "CITY_NAME",
          "mapping": "$.\"City_Name\"",
          "type": "DfString"
        },
        {
          "name": "CITY_CANONICAL_NAME",
          "mapping": "$.\"City_Canonical_Name\"",
          "type": "DfString"
        },
        {
          "name": "LAT_LON",
          "mapping": "$.\"Lat_Lon\"",
          "type": "DfString"
        },
        {
          "name": "START_TIME",
          "mapping": "$.\"Start_Time\"",
          "type": "DfInt64"
        },
        {
          "name": "END_TIME",
          "mapping": "$.\"End_Time\"",
          "type": "DfInt64"
        },
        {
          "name": "TEMPERATURE",
          "mapping": "$.\"Temperature\"",
          "type": "DfFloat64"
        },
        {
          "name": "PRESSURE",
          "mapping": "$.\"Pressure\"",
          "type": "DfFloat64"
        },
        {
          "name": "HUMIDITY",
          "mapping": "$.\"Humidity\"",
          "type": "DfInt64"
        },
        {
          "name": "WEATHER_ID",
          "mapping": "$.\"Weather_ID\"",
          "type": "DfInt64"
        }
      ]
    }
  },
  {
    "name": "test_23",
    "sample_file": "/saas-load-test/Error_Handling_Test/JSON_NO_Extension/json_no_extension",
    "dataset": "/saas-load-test/Error_Handling_Test/JSON_NO_Extension/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 2,
    "file_name_pattern": "*",
    "mark": pytest.mark.precheckin,
    "test_type": "schema",
    "recursive": True,
    "schema": {
  "rowpath": "$",
  "columns": [
    {
      "name": "A",
      "mapping": "$.\"A\"",
      "type": "DfInt64",
      "srcType": "JSON_INTEGER"
    },
    {
      "name": "B",
      "mapping": "$.\"B\"",
      "type": "DfInt64",
      "srcType": "JSON_INTEGER"
    },
    {
      "name": "C",
      "mapping": "$.\"C\"",
      "type": "DfInt64",
      "srcType": "JSON_INTEGER"
    },
    {
      "name": "D",
      "mapping": "$.\"D\"",
      "type": "DfInt64",
      "srcType": "JSON_INTEGER"
    },
    {
      "name": "E",
      "mapping": "$.\"E\"",
      "type": "DfInt64",
      "srcType": "JSON_INTEGER"
    }
  ]
}
  },
  {
    "name": "test_24",
    "sample_file": "/saas-load-test/Error_Handling_Test/Parquet_NO_Extension/parquet_no_extension",
    "dataset": "/saas-load-test/Error_Handling_Test/Parquet_NO_Extension/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 2,
    "file_name_pattern": "*",
    "test_type": "schema",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "schema" : {
  "rowpath": "$",
  "columns": [
    {
      "name": "A",
      "mapping": "$.\"a\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "B",
      "mapping": "$.\"b\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "C",
      "mapping": "$.\"c\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "D",
      "mapping": "$.\"d\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "E",
      "mapping": "$.\"e\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "F",
      "mapping": "$.\"f\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "G",
      "mapping": "$.\"g\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "H",
      "mapping": "$.\"h\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "I",
      "mapping": "$.\"i\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "J",
      "mapping": "$.\"j\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "K",
      "mapping": "$.\"k\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "L",
      "mapping": "$.\"l\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "M",
      "mapping": "$.\"m\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "N",
      "mapping": "$.\"n\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "O",
      "mapping": "$.\"o\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "P",
      "mapping": "$.\"p\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "Q",
      "mapping": "$.\"q\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "R",
      "mapping": "$.\"r\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "S",
      "mapping": "$.\"s\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "T",
      "mapping": "$.\"t\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "U",
      "mapping": "$.\"u\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "V",
      "mapping": "$.\"v\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "W",
      "mapping": "$.\"w\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "X",
      "mapping": "$.\"x\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "Y",
      "mapping": "$.\"y\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    },
    {
      "name": "Z",
      "mapping": "$.\"z\"",
      "type": "DfString",
      "srcType": "BYTE_ARRAY|UTF8"
    }
  ]
}
    },
  {
    "name": "test_25",
    "sample_file": "/saas-load-test/Error_Handling_Test/malformed_json/malformed.json",
    "dataset": "/saas-load-test/Error_Handling_Test/malformed_json/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.json",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "AWS ERROR: Error parsing JSON file. Please check the file and try again."
    },
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_26",
    "sample_file": "/saas-load-test/Error_Handling_Test/long_col/long_col.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/long_col/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
    "load": [
      {
        "THIS_IS_A_REALLY_LONG_COLUMN_NAME_THAT_DESERVES_SPECIAL_ATTENTION_SINCE_THE_WORLD_IS_SPINNING_OR_ELSE_MY_HEAD_IS_SPINNING_NOT_SURE_WHICH_ONE_IS_TRUE": "a",
        "SHORT_COL": "b",
        "ANOTHER_COLUMN": "c",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/long_col/long_col.csv"
      },
      {
        "THIS_IS_A_REALLY_LONG_COLUMN_NAME_THAT_DESERVES_SPECIAL_ATTENTION_SINCE_THE_WORLD_IS_SPINNING_OR_ELSE_MY_HEAD_IS_SPINNING_NOT_SURE_WHICH_ONE_IS_TRUE": "b",
        "SHORT_COL": "c",
        "ANOTHER_COLUMN": "d",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/long_col/long_col.csv"
      }
    ],
    "data": [
      {
        "THIS_IS_A_REALLY_LONG_COLUMN_NAME_THAT_DESERVES_SPECIAL_ATTENTION_SINCE_THE_WORLD_IS_SPINNING_OR_ELSE_MY_HEAD_IS_SPINNING_NOT_SURE_WHICH_ONE_IS_TRUE": "b",
        "SHORT_COL": "c",
        "ANOTHER_COLUMN": "d",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/long_col/long_col.csv",
        "XcalarRankOver": 2
      },
      {
        "THIS_IS_A_REALLY_LONG_COLUMN_NAME_THAT_DESERVES_SPECIAL_ATTENTION_SINCE_THE_WORLD_IS_SPINNING_OR_ELSE_MY_HEAD_IS_SPINNING_NOT_SURE_WHICH_ONE_IS_TRUE": "a",
        "SHORT_COL": "b",
        "ANOTHER_COLUMN": "c",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/long_col/long_col.csv",
        "XcalarRankOver": 1
      }
    ],
    "comp": []
  },
  {
    "name": "test_27",
    "sample_file": "/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext",
    "dataset": "/saas-load-test/Error_Handling_Test/gz_no_ext/",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
    "load": [
      {
        "ROW_NUMBER": 1,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 297.8,
        "PRESSURE": 1020.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext"
      },
      {
        "ROW_NUMBER": 2,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 299.17,
        "PRESSURE": 1023.2,
        "HUMIDITY": 69,
        "WEATHER_ID": 800,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext"
      },
      {
        "ROW_NUMBER": 3,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 300.13,
        "PRESSURE": 1021.92,
        "HUMIDITY": 64,
        "WEATHER_ID": 803,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext"
      }
    ],
    "data": [
      {
        "ROW_NUMBER": 2,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394690400,
        "END_TIME": 1394701199,
        "TEMPERATURE": 299.17,
        "PRESSURE": 1023.2,
        "HUMIDITY": 69,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext",
        "XcalarRankOver": 2
      },
      {
        "ROW_NUMBER": 1,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394679600,
        "END_TIME": 1394690399,
        "TEMPERATURE": 297.8,
        "PRESSURE": 1020.66,
        "HUMIDITY": 72,
        "WEATHER_ID": 800,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext",
        "XcalarRankOver": 1
      },
      {
        "ROW_NUMBER": 3,
        "CITY_NAME": "Ajman",
        "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
        "LAT_LON": "25.41111,55.43504",
        "START_TIME": 1394701200,
        "END_TIME": 1394711999,
        "TEMPERATURE": 300.13,
        "PRESSURE": 1021.92,
        "HUMIDITY": 64,
        "WEATHER_ID": 803,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/gz_no_ext/gz_no_ext",
        "XcalarRankOver": 3
      }
    ],
    "comp": []
  },
  {
    "name": "test_28",
    "sample_file": "/saas-load-test/Error_Handling_Test/gz_with_bz2_ext/gz_with_bz2_ext.bz2",
    "dataset": "/saas-load-test/Error_Handling_Test/gz_with_bz2_ext/",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*",
    "test_type": "global_status",
    "recursive": False,
    "global_status": {
      "error_message": "AWS ERROR: BZIP2 is not applicable to the queried object. Please correct the request and try again."
    },
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_29",
    "sample_file": "/saas-load-test/Error_Handling_Test/csv_with_json_ext/csv_with_json_ext.json",
    "dataset": "/saas-load-test/Error_Handling_Test/csv_with_json_ext/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.json",
    "mark": pytest.mark.precheckin,
    "test_type": "load",
    "recursive": True,
    "load": [
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_with_json_ext/csv_with_json_ext.json"
      },
      {
        "A": 4,
        "B": 2,
        "C": 5,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_with_json_ext/csv_with_json_ext.json"
      }
    ],
    "data": [
      {
        "A": 4,
        "B": 2,
        "C": 5,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_with_json_ext/csv_with_json_ext.json",
        "XcalarRankOver": 2
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_with_json_ext/csv_with_json_ext.json",
        "XcalarRankOver": 1
      }
    ],
    "comp": []
  },
  {
    "name": "test_30",
    "comment": "Ability to detect a parquet dataset, erroneous error as we are passing a prefix instead of key, as for parquet dataset it would make sense.",
    "sample_file": "/saas-load-test/Error_Handling_Test/ParquetDataset/",
    "dataset": "/saas-load-test/Error_Handling_Test/ParquetDataset/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "AWS ERROR: The specified key does not exist."
    },
    "mark": pytest.mark.xfail
  },
  {
    "name": "test_31",
    "comment": "Handling weird file names.",
    "sample_file": "/saas-load-test/Error_Handling_Test/Weird_Files_Names/we-_~a|m,; e1i#{}()<>hash+?%@.json",
    "dataset": "/saas-load-test/Error_Handling_Test/Weird_Files_Names/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
    "load": [
      {
        "A": 2,
        "B": 5,
        "E": 4,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Weird_Files_Names/we-_~a|m,; e1i#{}()<>hash+?%@.json"
      }
    ],
    "data": [
      {
        "A": 2,
        "B": 5,
        "E": 4,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/Weird_Files_Names/we-_~a|m,; e1i#{}()<>hash+?%@.json",
        "XcalarRankOver": 1
      }
    ],
    "comp": []
  },
  {
    "name": "test_32",
    "comment": "Complements case: Parquet with no columnar compression but has timestamp column that exceeds range.",
    "sample_file": "/saas-load-test/Error_Handling_Test/alltypes_parquet/alltypes_dictionary.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/alltypes_parquet/alltypes_dictionary.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "recursive": False,
    "mark": pytest.mark.precheckin,
"load" : [
  {
    "ID": 0,
    "BOOL_COL": True,
    "TINYINT_COL": 0,
    "SMALLINT_COL": 0,
    "INT_COL": 0,
    "BIGINT_COL": 0,
    "FLOAT_COL": 0.0,
    "DOUBLE_COL": 0.0,
    "DATE_STRING_COL": "MDEvMDEvMDk=",
    "STRING_COL": "MA==",
    "TIMESTAMP_COL": "45283676094696639722160128",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_parquet/alltypes_dictionary.parquet"
  },
  {
    "ID": 1,
    "BOOL_COL": False,
    "TINYINT_COL": 1,
    "SMALLINT_COL": 1,
    "INT_COL": 1,
    "BIGINT_COL": 10,
    "FLOAT_COL": 1.100000023841858,
    "DOUBLE_COL": 10.1,
    "DATE_STRING_COL": "MDEvMDEvMDk=",
    "STRING_COL": "MQ==",
    "TIMESTAMP_COL": "45283676094696699722160128",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_parquet/alltypes_dictionary.parquet"
  }
],
"data" : [
  {
    "ID": 1,
    "BOOL_COL": False,
    "TINYINT_COL": 1,
    "SMALLINT_COL": 1,
    "INT_COL": 1,
    "BIGINT_COL": 10,
    "FLOAT_COL": 1.100000023841858,
    "DOUBLE_COL": 10.1,
    "DATE_STRING_COL": "MDEvMDEvMDk=",
    "STRING_COL": "MQ==",
    "TIMESTAMP_COL": "45283676094696699722160128",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_parquet/alltypes_dictionary.parquet",
    "XcalarRankOver": 2
  },
  {
    "ID": 0,
    "BOOL_COL": True,
    "TINYINT_COL": 0,
    "SMALLINT_COL": 0,
    "INT_COL": 0,
    "BIGINT_COL": 0,
    "FLOAT_COL": 0.0,
    "DOUBLE_COL": 0.0,
    "DATE_STRING_COL": "MDEvMDEvMDk=",
    "STRING_COL": "MA==",
    "TIMESTAMP_COL": "45283676094696639722160128",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_parquet/alltypes_dictionary.parquet",
    "XcalarRankOver": 1
  }
],
"comp" : []
  },
  {
    "name": "test_33",
    "comment": "Create complements table, Parquet with columnar compression and with timestamp column that exceeds range.",
    "sample_file": "/saas-load-test/Error_Handling_Test/alltypes_snappy_parquet/alltypes_plain.snappy.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/alltypes_snappy_parquet/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 5,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
"load" : [
  {
    "ID": 6,
    "BOOL_COL": True,
    "TINYINT_COL": 0,
    "SMALLINT_COL": 0,
    "INT_COL": 0,
    "BIGINT_COL": 0,
    "FLOAT_COL": 0.0,
    "DOUBLE_COL": 0.0,
    "DATE_STRING_COL": "MDQvMDEvMDk=",
    "STRING_COL": "MA==",
    "TIMESTAMP_COL": "45285336301663273581805568",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_snappy_parquet/alltypes_plain.snappy.parquet"
  },
  {
    "ID": 7,
    "BOOL_COL": False,
    "TINYINT_COL": 1,
    "SMALLINT_COL": 1,
    "INT_COL": 1,
    "BIGINT_COL": 10,
    "FLOAT_COL": 1.100000023841858,
    "DOUBLE_COL": 10.1,
    "DATE_STRING_COL": "MDQvMDEvMDk=",
    "STRING_COL": "MQ==",
    "TIMESTAMP_COL": "45285336301663333581805568",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_snappy_parquet/alltypes_plain.snappy.parquet"
  }
],
"data" : [
  {
    "ID": 7,
    "BOOL_COL": False,
    "TINYINT_COL": 1,
    "SMALLINT_COL": 1,
    "INT_COL": 1,
    "BIGINT_COL": 10,
    "FLOAT_COL": 1.100000023841858,
    "DOUBLE_COL": 10.1,
    "DATE_STRING_COL": "MDQvMDEvMDk=",
    "STRING_COL": "MQ==",
    "TIMESTAMP_COL": "45285336301663333581805568",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_snappy_parquet/alltypes_plain.snappy.parquet",
    "XcalarRankOver": 2
  },
  {
    "ID": 6,
    "BOOL_COL": True,
    "TINYINT_COL": 0,
    "SMALLINT_COL": 0,
    "INT_COL": 0,
    "BIGINT_COL": 0,
    "FLOAT_COL": 0.0,
    "DOUBLE_COL": 0.0,
    "DATE_STRING_COL": "MDQvMDEvMDk=",
    "STRING_COL": "MA==",
    "TIMESTAMP_COL": "45285336301663273581805568",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/alltypes_snappy_parquet/alltypes_plain.snappy.parquet",
    "XcalarRankOver": 1
  }
],
"comp" : []
  },
  {
    "name": "test_34",
    "comment": "ENG-9724: Handling gzip columnar compression. May have potential base64 encoding issues though!",
    "sample_file": "/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 5,
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.precheckin,
    "test_type": "load",
    "recursive": True,
    "load": [
      {
        "N_NATIONKEY": 0,
        "N_NAME": "QUxHRVJJQQ==",
        "N_REGIONKEY": 0,
        "N_COMMENT": "IGhhZ2dsZS4gY2FyZWZ1bGx5IGZpbmFsIGRlcG9zaXRzIGRldGVjdCBzbHlseSBhZ2Fp",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 1,
        "N_NAME": "QVJHRU5USU5B",
        "N_REGIONKEY": 1,
        "N_COMMENT": "YWwgZm94ZXMgcHJvbWlzZSBzbHlseSBhY2NvcmRpbmcgdG8gdGhlIHJlZ3VsYXIgYWNjb3VudHMuIGJvbGQgcmVxdWVzdHMgYWxvbg==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 2,
        "N_NAME": "QlJBWklM",
        "N_REGIONKEY": 1,
        "N_COMMENT": "eSBhbG9uZ3NpZGUgb2YgdGhlIHBlbmRpbmcgZGVwb3NpdHMuIGNhcmVmdWxseSBzcGVjaWFsIHBhY2thZ2VzIGFyZSBhYm91dCB0aGUgaXJvbmljIGZvcmdlcy4gc2x5bHkgc3BlY2lhbCA=",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 3,
        "N_NAME": "Q0FOQURB",
        "N_REGIONKEY": 1,
        "N_COMMENT": "ZWFzIGhhbmcgaXJvbmljLCBzaWxlbnQgcGFja2FnZXMuIHNseWx5IHJlZ3VsYXIgcGFja2FnZXMgYXJlIGZ1cmlvdXNseSBvdmVyIHRoZSB0aXRoZXMuIGZsdWZmaWx5IGJvbGQ=",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 4,
        "N_NAME": "RUdZUFQ=",
        "N_REGIONKEY": 4,
        "N_COMMENT": "eSBhYm92ZSB0aGUgY2FyZWZ1bGx5IHVudXN1YWwgdGhlb2RvbGl0ZXMuIGZpbmFsIGR1Z291dHMgYXJlIHF1aWNrbHkgYWNyb3NzIHRoZSBmdXJpb3VzbHkgcmVndWxhciBk",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 5,
        "N_NAME": "RVRISU9QSUE=",
        "N_REGIONKEY": 0,
        "N_COMMENT": "dmVuIHBhY2thZ2VzIHdha2UgcXVpY2tseS4gcmVndQ==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 6,
        "N_NAME": "RlJBTkNF",
        "N_REGIONKEY": 3,
        "N_COMMENT": "cmVmdWxseSBmaW5hbCByZXF1ZXN0cy4gcmVndWxhciwgaXJvbmk=",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 7,
        "N_NAME": "R0VSTUFOWQ==",
        "N_REGIONKEY": 3,
        "N_COMMENT": "bCBwbGF0ZWxldHMuIHJlZ3VsYXIgYWNjb3VudHMgeC1yYXk6IHVudXN1YWwsIHJlZ3VsYXIgYWNjbw==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 8,
        "N_NAME": "SU5ESUE=",
        "N_REGIONKEY": 2,
        "N_COMMENT": "c3MgZXhjdXNlcyBjYWpvbGUgc2x5bHkgYWNyb3NzIHRoZSBwYWNrYWdlcy4gZGVwb3NpdHMgcHJpbnQgYXJvdW4=",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 9,
        "N_NAME": "SU5ET05FU0lB",
        "N_REGIONKEY": 2,
        "N_COMMENT": "IHNseWx5IGV4cHJlc3MgYXN5bXB0b3Rlcy4gcmVndWxhciBkZXBvc2l0cyBoYWdnbGUgc2x5bHkuIGNhcmVmdWxseSBpcm9uaWMgaG9ja2V5IHBsYXllcnMgc2xlZXAgYmxpdGhlbHkuIGNhcmVmdWxs",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 10,
        "N_NAME": "SVJBTg==",
        "N_REGIONKEY": 4,
        "N_COMMENT": "ZWZ1bGx5IGFsb25nc2lkZSBvZiB0aGUgc2x5bHkgZmluYWwgZGVwZW5kZW5jaWVzLiA=",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 11,
        "N_NAME": "SVJBUQ==",
        "N_REGIONKEY": 4,
        "N_COMMENT": "bmljIGRlcG9zaXRzIGJvb3N0IGF0b3AgdGhlIHF1aWNrbHkgZmluYWwgcmVxdWVzdHM/IHF1aWNrbHkgcmVndWxh",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 12,
        "N_NAME": "SkFQQU4=",
        "N_REGIONKEY": 2,
        "N_COMMENT": "b3VzbHkuIGZpbmFsLCBleHByZXNzIGdpZnRzIGNham9sZSBh",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 13,
        "N_NAME": "Sk9SREFO",
        "N_REGIONKEY": 4,
        "N_COMMENT": "aWMgZGVwb3NpdHMgYXJlIGJsaXRoZWx5IGFib3V0IHRoZSBjYXJlZnVsbHkgcmVndWxhciBwYQ==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 14,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 14,
        "N_NAME": "S0VOWUE=",
        "N_REGIONKEY": 0,
        "N_COMMENT": "IHBlbmRpbmcgZXhjdXNlcyBoYWdnbGUgZnVyaW91c2x5IGRlcG9zaXRzLiBwZW5kaW5nLCBleHByZXNzIHBpbnRvIGJlYW5zIHdha2UgZmx1ZmZpbHkgcGFzdCB0",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 15,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 15,
        "N_NAME": "TU9ST0NDTw==",
        "N_REGIONKEY": 0,
        "N_COMMENT": "cm5zLiBibGl0aGVseSBib2xkIGNvdXJ0cyBhbW9uZyB0aGUgY2xvc2VseSByZWd1bGFyIHBhY2thZ2VzIHVzZSBmdXJpb3VzbHkgYm9sZCBwbGF0ZWxldHM/",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 16,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 16,
        "N_NAME": "TU9aQU1CSVFVRQ==",
        "N_REGIONKEY": 0,
        "N_COMMENT": "cy4gaXJvbmljLCB1bnVzdWFsIGFzeW1wdG90ZXMgd2FrZSBibGl0aGVseSBy",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 17,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 17,
        "N_NAME": "UEVSVQ==",
        "N_REGIONKEY": 1,
        "N_COMMENT": "cGxhdGVsZXRzLiBibGl0aGVseSBwZW5kaW5nIGRlcGVuZGVuY2llcyB1c2UgZmx1ZmZpbHkgYWNyb3NzIHRoZSBldmVuIHBpbnRvIGJlYW5zLiBjYXJlZnVsbHkgc2lsZW50IGFjY291bg==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 18,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 18,
        "N_NAME": "Q0hJTkE=",
        "N_REGIONKEY": 2,
        "N_COMMENT": "YyBkZXBlbmRlbmNpZXMuIGZ1cmlvdXNseSBleHByZXNzIG5vdG9ybmlzIHNsZWVwIHNseWx5IHJlZ3VsYXIgYWNjb3VudHMuIGlkZWFzIHNsZWVwLiBkZXBvcw==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 19,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 19,
        "N_NAME": "Uk9NQU5JQQ==",
        "N_REGIONKEY": 3,
        "N_COMMENT": "dWxhciBhc3ltcHRvdGVzIGFyZSBhYm91dCB0aGUgZnVyaW91cyBtdWx0aXBsaWVycy4gZXhwcmVzcyBkZXBlbmRlbmNpZXMgbmFnIGFib3ZlIHRoZSBpcm9uaWNhbGx5IGlyb25pYyBhY2NvdW50",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 20,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 20,
        "N_NAME": "U0FVREkgQVJBQklB",
        "N_REGIONKEY": 4,
        "N_COMMENT": "dHMuIHNpbGVudCByZXF1ZXN0cyBoYWdnbGUuIGNsb3NlbHkgZXhwcmVzcyBwYWNrYWdlcyBzbGVlcCBhY3Jvc3MgdGhlIGJsaXRoZWx5",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 21,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 21,
        "N_NAME": "VklFVE5BTQ==",
        "N_REGIONKEY": 2,
        "N_COMMENT": "aGVseSBlbnRpY2luZ2x5IGV4cHJlc3MgYWNjb3VudHMuIGV2ZW4sIGZpbmFsIA==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 22,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 22,
        "N_NAME": "UlVTU0lB",
        "N_REGIONKEY": 3,
        "N_COMMENT": "IHJlcXVlc3RzIGFnYWluc3QgdGhlIHBsYXRlbGV0cyB1c2UgbmV2ZXIgYWNjb3JkaW5nIHRvIHRoZSBxdWlja2x5IHJlZ3VsYXIgcGludA==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 23,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 23,
        "N_NAME": "VU5JVEVEIEtJTkdET00=",
        "N_REGIONKEY": 3,
        "N_COMMENT": "ZWFucyBib29zdCBjYXJlZnVsbHkgc3BlY2lhbCByZXF1ZXN0cy4gYWNjb3VudHMgYXJlLiBjYXJlZnVsbA==",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 24,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      },
      {
        "N_NATIONKEY": 24,
        "N_NAME": "VU5JVEVEIFNUQVRFUw==",
        "N_REGIONKEY": 1,
        "N_COMMENT": "eSBmaW5hbCBwYWNrYWdlcy4gc2xvdyBmb3hlcyBjYWpvbGUgcXVpY2tseS4gcXVpY2tseSBzaWxlbnQgcGxhdGVsZXRzIGJyZWFjaCBpcm9uaWMgYWNjb3VudHMuIHVudXN1YWwgcGludG8gYmU=",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 25,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet"
      }
    ],
    "data": [
      {
        "N_NATIONKEY": 7,
        "N_NAME": "R0VSTUFOWQ==",
        "N_REGIONKEY": 3,
        "N_COMMENT": "bCBwbGF0ZWxldHMuIHJlZ3VsYXIgYWNjb3VudHMgeC1yYXk6IHVudXN1YWwsIHJlZ3VsYXIgYWNjbw==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 8
      },
      {
        "N_NATIONKEY": 11,
        "N_NAME": "SVJBUQ==",
        "N_REGIONKEY": 4,
        "N_COMMENT": "bmljIGRlcG9zaXRzIGJvb3N0IGF0b3AgdGhlIHF1aWNrbHkgZmluYWwgcmVxdWVzdHM/IHF1aWNrbHkgcmVndWxh",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 12
      },
      {
        "N_NATIONKEY": 3,
        "N_NAME": "Q0FOQURB",
        "N_REGIONKEY": 1,
        "N_COMMENT": "ZWFzIGhhbmcgaXJvbmljLCBzaWxlbnQgcGFja2FnZXMuIHNseWx5IHJlZ3VsYXIgcGFja2FnZXMgYXJlIGZ1cmlvdXNseSBvdmVyIHRoZSB0aXRoZXMuIGZsdWZmaWx5IGJvbGQ=",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 4
      },
      {
        "N_NATIONKEY": 15,
        "N_NAME": "TU9ST0NDTw==",
        "N_REGIONKEY": 0,
        "N_COMMENT": "cm5zLiBibGl0aGVseSBib2xkIGNvdXJ0cyBhbW9uZyB0aGUgY2xvc2VseSByZWd1bGFyIHBhY2thZ2VzIHVzZSBmdXJpb3VzbHkgYm9sZCBwbGF0ZWxldHM/",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 16
      },
      {
        "N_NATIONKEY": 8,
        "N_NAME": "SU5ESUE=",
        "N_REGIONKEY": 2,
        "N_COMMENT": "c3MgZXhjdXNlcyBjYWpvbGUgc2x5bHkgYWNyb3NzIHRoZSBwYWNrYWdlcy4gZGVwb3NpdHMgcHJpbnQgYXJvdW4=",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 9
      },
      {
        "N_NATIONKEY": 6,
        "N_NAME": "RlJBTkNF",
        "N_REGIONKEY": 3,
        "N_COMMENT": "cmVmdWxseSBmaW5hbCByZXF1ZXN0cy4gcmVndWxhciwgaXJvbmk=",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 7
      },
      {
        "N_NATIONKEY": 22,
        "N_NAME": "UlVTU0lB",
        "N_REGIONKEY": 3,
        "N_COMMENT": "IHJlcXVlc3RzIGFnYWluc3QgdGhlIHBsYXRlbGV0cyB1c2UgbmV2ZXIgYWNjb3JkaW5nIHRvIHRoZSBxdWlja2x5IHJlZ3VsYXIgcGludA==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 23
      },
      {
        "N_NATIONKEY": 16,
        "N_NAME": "TU9aQU1CSVFVRQ==",
        "N_REGIONKEY": 0,
        "N_COMMENT": "cy4gaXJvbmljLCB1bnVzdWFsIGFzeW1wdG90ZXMgd2FrZSBibGl0aGVseSBy",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 17
      },
      {
        "N_NATIONKEY": 17,
        "N_NAME": "UEVSVQ==",
        "N_REGIONKEY": 1,
        "N_COMMENT": "cGxhdGVsZXRzLiBibGl0aGVseSBwZW5kaW5nIGRlcGVuZGVuY2llcyB1c2UgZmx1ZmZpbHkgYWNyb3NzIHRoZSBldmVuIHBpbnRvIGJlYW5zLiBjYXJlZnVsbHkgc2lsZW50IGFjY291bg==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 18
      },
      {
        "N_NATIONKEY": 1,
        "N_NAME": "QVJHRU5USU5B",
        "N_REGIONKEY": 1,
        "N_COMMENT": "YWwgZm94ZXMgcHJvbWlzZSBzbHlseSBhY2NvcmRpbmcgdG8gdGhlIHJlZ3VsYXIgYWNjb3VudHMuIGJvbGQgcmVxdWVzdHMgYWxvbg==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 2
      },
      {
        "N_NATIONKEY": 0,
        "N_NAME": "QUxHRVJJQQ==",
        "N_REGIONKEY": 0,
        "N_COMMENT": "IGhhZ2dsZS4gY2FyZWZ1bGx5IGZpbmFsIGRlcG9zaXRzIGRldGVjdCBzbHlseSBhZ2Fp",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 1
      },
      {
        "N_NATIONKEY": 10,
        "N_NAME": "SVJBTg==",
        "N_REGIONKEY": 4,
        "N_COMMENT": "ZWZ1bGx5IGFsb25nc2lkZSBvZiB0aGUgc2x5bHkgZmluYWwgZGVwZW5kZW5jaWVzLiA=",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 11
      },
      {
        "N_NATIONKEY": 24,
        "N_NAME": "VU5JVEVEIFNUQVRFUw==",
        "N_REGIONKEY": 1,
        "N_COMMENT": "eSBmaW5hbCBwYWNrYWdlcy4gc2xvdyBmb3hlcyBjYWpvbGUgcXVpY2tseS4gcXVpY2tseSBzaWxlbnQgcGxhdGVsZXRzIGJyZWFjaCBpcm9uaWMgYWNjb3VudHMuIHVudXN1YWwgcGludG8gYmU=",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 25
      },
      {
        "N_NATIONKEY": 2,
        "N_NAME": "QlJBWklM",
        "N_REGIONKEY": 1,
        "N_COMMENT": "eSBhbG9uZ3NpZGUgb2YgdGhlIHBlbmRpbmcgZGVwb3NpdHMuIGNhcmVmdWxseSBzcGVjaWFsIHBhY2thZ2VzIGFyZSBhYm91dCB0aGUgaXJvbmljIGZvcmdlcy4gc2x5bHkgc3BlY2lhbCA=",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 3
      },
      {
        "N_NATIONKEY": 12,
        "N_NAME": "SkFQQU4=",
        "N_REGIONKEY": 2,
        "N_COMMENT": "b3VzbHkuIGZpbmFsLCBleHByZXNzIGdpZnRzIGNham9sZSBh",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 13
      },
      {
        "N_NATIONKEY": 5,
        "N_NAME": "RVRISU9QSUE=",
        "N_REGIONKEY": 0,
        "N_COMMENT": "dmVuIHBhY2thZ2VzIHdha2UgcXVpY2tseS4gcmVndQ==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 6
      },
      {
        "N_NATIONKEY": 9,
        "N_NAME": "SU5ET05FU0lB",
        "N_REGIONKEY": 2,
        "N_COMMENT": "IHNseWx5IGV4cHJlc3MgYXN5bXB0b3Rlcy4gcmVndWxhciBkZXBvc2l0cyBoYWdnbGUgc2x5bHkuIGNhcmVmdWxseSBpcm9uaWMgaG9ja2V5IHBsYXllcnMgc2xlZXAgYmxpdGhlbHkuIGNhcmVmdWxs",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 10
      },
      {
        "N_NATIONKEY": 14,
        "N_NAME": "S0VOWUE=",
        "N_REGIONKEY": 0,
        "N_COMMENT": "IHBlbmRpbmcgZXhjdXNlcyBoYWdnbGUgZnVyaW91c2x5IGRlcG9zaXRzLiBwZW5kaW5nLCBleHByZXNzIHBpbnRvIGJlYW5zIHdha2UgZmx1ZmZpbHkgcGFzdCB0",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 15
      },
      {
        "N_NATIONKEY": 4,
        "N_NAME": "RUdZUFQ=",
        "N_REGIONKEY": 4,
        "N_COMMENT": "eSBhYm92ZSB0aGUgY2FyZWZ1bGx5IHVudXN1YWwgdGhlb2RvbGl0ZXMuIGZpbmFsIGR1Z291dHMgYXJlIHF1aWNrbHkgYWNyb3NzIHRoZSBmdXJpb3VzbHkgcmVndWxhciBk",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 5
      },
      {
        "N_NATIONKEY": 21,
        "N_NAME": "VklFVE5BTQ==",
        "N_REGIONKEY": 2,
        "N_COMMENT": "aGVseSBlbnRpY2luZ2x5IGV4cHJlc3MgYWNjb3VudHMuIGV2ZW4sIGZpbmFsIA==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 22
      },
      {
        "N_NATIONKEY": 23,
        "N_NAME": "VU5JVEVEIEtJTkdET00=",
        "N_REGIONKEY": 3,
        "N_COMMENT": "ZWFucyBib29zdCBjYXJlZnVsbHkgc3BlY2lhbCByZXF1ZXN0cy4gYWNjb3VudHMgYXJlLiBjYXJlZnVsbA==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 24
      },
      {
        "N_NATIONKEY": 13,
        "N_NAME": "Sk9SREFO",
        "N_REGIONKEY": 4,
        "N_COMMENT": "aWMgZGVwb3NpdHMgYXJlIGJsaXRoZWx5IGFib3V0IHRoZSBjYXJlZnVsbHkgcmVndWxhciBwYQ==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 14
      },
      {
        "N_NATIONKEY": 20,
        "N_NAME": "U0FVREkgQVJBQklB",
        "N_REGIONKEY": 4,
        "N_COMMENT": "dHMuIHNpbGVudCByZXF1ZXN0cyBoYWdnbGUuIGNsb3NlbHkgZXhwcmVzcyBwYWNrYWdlcyBzbGVlcCBhY3Jvc3MgdGhlIGJsaXRoZWx5",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 21
      },
      {
        "N_NATIONKEY": 19,
        "N_NAME": "Uk9NQU5JQQ==",
        "N_REGIONKEY": 3,
        "N_COMMENT": "dWxhciBhc3ltcHRvdGVzIGFyZSBhYm91dCB0aGUgZnVyaW91cyBtdWx0aXBsaWVycy4gZXhwcmVzcyBkZXBlbmRlbmNpZXMgbmFnIGFib3ZlIHRoZSBpcm9uaWNhbGx5IGlyb25pYyBhY2NvdW50",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 20
      },
      {
        "N_NATIONKEY": 18,
        "N_NAME": "Q0hJTkE=",
        "N_REGIONKEY": 2,
        "N_COMMENT": "YyBkZXBlbmRlbmNpZXMuIGZ1cmlvdXNseSBleHByZXNzIG5vdG9ybmlzIHNsZWVwIHNseWx5IHJlZ3VsYXIgYWNjb3VudHMuIGlkZWFzIHNsZWVwLiBkZXBvcw==",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/columnar_gzip_parquet/gzip-nation.impala.parquet",
        "XcalarRankOver": 19
      }
    ],
    "comp": []
  },
  {
    "name": "test_35",
    "comment": "Fetching array columns since array column in chosen schema.",
    "sample_file": "/saas-load-test/Error_Handling_Test/jsonl_array_fields/line_array_fields.json",
    "dataset": "/saas-load-test/Error_Handling_Test/jsonl_array_fields/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 5,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
"load" : [
  {
    "A": 2,
    "B": 5,
    "C_0": 2,
    "C_1": 2,
    "C_2": 3,
    "C_3": 1,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields/line_array_fields.json"
  },
  {
    "A": 3,
    "B": 4,
    "C_0": 2,
    "C_1": 5,
    "C_2": 2,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields/line_array_fields.json"
  }
],
"data" : [
  {
    "A": 3,
    "B": 4,
    "C_0": 2,
    "C_1": 5,
    "C_2": 2,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields/line_array_fields.json",
    "XcalarRankOver": 2
  },
  {
    "A": 2,
    "B": 5,
    "C_0": 2,
    "C_1": 2,
    "C_2": 3,
    "C_3": 1,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields/line_array_fields.json",
    "XcalarRankOver": 1
  }
],
"comp" : []
  },
  {
    "name": "test_36",
    "comment": "Column 'c' has an object value in one record and an int value in another.",
    "sample_file": "/saas-load-test/Error_Handling_Test/nested_dup_inconsistent_cols/nested_dup_inconsistent_cols.jsonl",
    "dataset": "/saas-load-test/Error_Handling_Test/nested_dup_inconsistent_cols/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 5,
    "file_name_pattern": "*.jsonl",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "AWS ERROR: Error parsing JSON file. Please check the file and try again."
    },
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_37",
    "comment": "Handling a multi-line record.",
    "sample_file": "/saas-load-test/Error_Handling_Test/multi_line_record/multi_line_record.json",
    "dataset": "/saas-load-test/Error_Handling_Test/multi_line_record/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 5,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
    "load": [
      {
        "A": 2,
        "B": 5,
        "C": 3,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/multi_line_record/multi_line_record.json"
      }
    ],
    "data": [
      {
        "A": 2,
        "B": 5,
        "C": 3,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/multi_line_record/multi_line_record.json",
        "XcalarRankOver": 1
      }
    ],
    "comp": []
  },
  {
    "name": "test_38",
    "comment": "Handling an array inside an object value.",
    "sample_file": "/saas-load-test/Error_Handling_Test/array_in_object/array_in_object.jsonl",
    "dataset": "/saas-load-test/Error_Handling_Test/array_in_object/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.jsonl",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
"load" : [
  {
    "A": 2,
    "B": 5,
    "D_0": 1,
    "D_1": 2,
    "D_2": 3,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/array_in_object/array_in_object.jsonl"
  }
],
"data" : [
  {
    "A": 2,
    "B": 5,
    "D_0": 1,
    "D_1": 2,
    "D_2": 3,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/array_in_object/array_in_object.jsonl",
    "XcalarRankOver": 1
  }
],
"comp" : []
  },
  {
    "name": "test_39",
    "comment": "Handling case sensitive of the key.",
    "sample_file": "/saas-load-test/Error_Handling_Test/case_sensitive_test/CASE_SENSITIVE_KEY.JSON",
    "dataset": "/saas-load-test/Error_Handling_Test/case_sensitive_test/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
    "load": [
      {
        "A": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/case_sensitive_test/CASE_SENSITIVE_KEY.JSON"
      },
      {
        "A": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/case_sensitive_test/CaSe_SENSiTIVe_KEy.JsON"
      },
      {
        "A": 1,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/case_sensitive_test/case_sensitive_key.json"
      }
    ],
    "data": [
      {
        "A": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/case_sensitive_test/CaSe_SENSiTIVe_KEy.JsON",
        "XcalarRankOver": 2
      },
      {
        "A": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/case_sensitive_test/CASE_SENSITIVE_KEY.JSON",
        "XcalarRankOver": 1
      },
      {
        "A": 1,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/case_sensitive_test/case_sensitive_key.json",
        "XcalarRankOver": 3
      }
    ],
    "comp": []
  },
  {
    "name": "test_40",
    "comment": "ENG-9723 - Valid JSONL gets categorized as array json.",
    "sample_file": "/saas-load-test/Error_Handling_Test/edge_case_array/edge_case_array.json",
    "dataset": "/saas-load-test/Error_Handling_Test/edge_case_array/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "DATA ERROR: This JSON file appears to start with an array. Only JSONL is currently supported."
    },
    "mark": pytest.mark.xfail
  },
  {
    "name": "test_41",
    "comment": "Handling a corrupted parquet file.",
    "sample_file": "/saas-load-test/Error_Handling_Test/corrupted_parquet/corrupted.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/corrupted_parquet/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.parquet",
    "test_type": "global_status",
    "recursive": True,
    "global_status": {
      "error_message": "AWS ERROR: UTF-8 encoding is required. The text encoding error was found near byte 8,192."
    },
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_42",
    "comment": "Inconsistent delimiters, missing trailing values on some rows.",
    "sample_file": "/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/header_data_mismatch/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.precheckin,
    "test_type": "load",
    "recursive": True,
    "load": [
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 4,
        "F": 5,
        "G": 6,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv"
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 5,
        "F": 6,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv"
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv"
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 2,
        "F": 2,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv"
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv"
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv"
      }
    ],
    "data": [
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 2,
        "F": 2,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
        "XcalarRankOver": 4
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 5,
        "F": 6,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
        "XcalarRankOver": 2
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 4,
        "F": 5,
        "G": 6,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
        "XcalarRankOver": 1
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
        "XcalarRankOver": 3
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
        "XcalarRankOver": 6
      },
      {
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/header_data_mismatch/header_data_mismatch.csv",
        "XcalarRankOver": 5
      }
    ],
    "comp": []
  },
  {
    "name": "test_43",
    "comment": "ENG-9698 - Needs an error like 'unsupported format'",
    "sample_file": "/saas-load-test/Error_Handling_Test/archive/archive.tar.gz",
    "dataset": "/saas-load-test/Error_Handling_Test/archive/",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.gz",
    "test_type": "load",
    "recursive": True,
    "load": {},
    "mark": pytest.mark.xfail
  },
  {
    "name": "test_44",
    "comment": "Marking xfail: Double quote inside a column name",
    "sample_file": "/saas-load-test/Error_Handling_Test/Quoted_Column/Quoted_Column.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/Quoted_Column/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 5,
    "file_name_pattern": "*.csv",
    "test_type": "schema",
    "recursive": True,
    "mark": pytest.mark.xfail,
    "schema": {
      "rowpath": "$",
      "columns": [
        {
          "name": "ROW_NUMBER",
          "mapping": "$.\"Row_Number\"",
          "type": "DfInt64"
        },
        {
          "name": "CITY__NAME",
          "mapping": "$.\"City\"_Name\"",
          "type": "DfString"
        },
        {
          "name": "CITY_CANONICAL_NAME",
          "mapping": "$.\"City_Canonical_Name\"",
          "type": "DfString"
        },
        {
          "name": "LAT_LON",
          "mapping": "$.\"Lat_Lon\"",
          "type": "DfString"
        },
        {
          "name": "START_TIME",
          "mapping": "$.\"Start_Time\"",
          "type": "DfInt64"
        },
        {
          "name": "END_TIME",
          "mapping": "$.\"End_Time\"",
          "type": "DfInt64"
        },
        {
          "name": "TEMPERATURE",
          "mapping": "$.\"Temperature\"",
          "type": "DfFloat64"
        },
        {
          "name": "PRESSURE",
          "mapping": "$.\"Pressure\"",
          "type": "DfFloat64"
        },
        {
          "name": "HUMIDITY",
          "mapping": "$.\"Humidity\"",
          "type": "DfInt64"
        },
        {
          "name": "WEATHER_ID",
          "mapping": "$.\"Weather_ID\"",
          "type": "DfInt64"
        }
      ]
    }
  },
  {
    "name": "test_45",
    "comment": "User chooses schema where 'c' is an array. This works as 'c' is excluded during load, even if some rows have it as int.",
    "sample_file": "/saas-load-test/Error_Handling_Test/jsonl_array_fields_some_rows/line_array_fields_some_rows.json",
    "dataset": "/saas-load-test/Error_Handling_Test/jsonl_array_fields_some_rows/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 5,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
"load" : [
  {
    "A": 3,
    "B": 4,
    "C_0": 2,
    "C_1": 5,
    "C_2": 2,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields_some_rows/line_array_fields_some_rows.json"
  },
  {
    "A": 2,
    "B": 5,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields_some_rows/line_array_fields_some_rows.json"
  }
],
"data" : [
  {
    "A": 2,
    "B": 5,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields_some_rows/line_array_fields_some_rows.json",
    "XcalarRankOver": 2
  },
  {
    "A": 3,
    "B": 4,
    "C_0": 2,
    "C_1": 5,
    "C_2": 2,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_fields_some_rows/line_array_fields_some_rows.json",
    "XcalarRankOver": 1
  }
],
"comp" : []
  },
  {
    "name": "test_46",
    "comment": "User chooses schema where 'c' is int, but during load it is an array. This is not supported",
    "sample_file": "/saas-load-test/Error_Handling_Test/jsonl_array_mixed_fields/jsonl_array_mixed_fields.json",
    "dataset": "/saas-load-test/Error_Handling_Test/jsonl_array_mixed_fields/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": False,
"load" : [
  {
    "A": 2,
    "B": 5,
    "C": 5,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_mixed_fields/jsonl_array_mixed_fields.json"
  },
  {
    "XCALAR_ICV": "ERROR: Found Array in column(C), expected(DfInt64)",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': 3, 'B': 4, 'C': [2, 5, 2]}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_mixed_fields/jsonl_array_mixed_fields.json"
  }
],
"data" : [
  {
    "A": 2,
    "B": 5,
    "C": 5,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_mixed_fields/jsonl_array_mixed_fields.json",
    "XcalarRankOver": 1
  }
],
"comp" : [
  {
    "XCALAR_ICV": "ERROR: Found Array in column(C), expected(DfInt64)",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "[TRUNCATED] {'A': 3, 'B': 4, 'C': [2, 5, 2]}",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/jsonl_array_mixed_fields/jsonl_array_mixed_fields.json",
    "XcalarRankOver": 1
  }
]
  },
  {
    "name": "test_47",
    "comment": "Ignoring delimiter inside double quoted values.",
    "sample_file": "/saas-load-test/Error_Handling_Test/schema_detect_str/schema_detect_str.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/schema_detect_str/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "recursive": True,
    "mark": pytest.mark.precheckin,
"load" : [
  {
    "ROW_NUMBER": 1,
    "CITY_NAME": "Ajman",
    "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
    "LAT_LON": "25.41111,55.43504",
    "START_TIME": 1394679600,
    "END_TIME": 1394690399,
    "TEMPERATURE": 297.8,
    "PRESSURE": 1020.66,
    "HUMIDITY": 72,
    "WEATHER_ID": 800,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/schema_detect_str/schema_detect_str.csv"
  },
  {
    "ROW_NUMBER": 2,
    "CITY_NAME": "Ajman",
    "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
    "LAT_LON": "25.41111,55.43504",
    "START_TIME": 1394690400,
    "END_TIME": 1394701199,
    "TEMPERATURE": 299.17,
    "PRESSURE": 1023.2,
    "HUMIDITY": 69,
    "WEATHER_ID": 800,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/schema_detect_str/schema_detect_str.csv"
  }
],
"data" : [
  {
    "ROW_NUMBER": 2,
    "CITY_NAME": "Ajman",
    "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
    "LAT_LON": "25.41111,55.43504",
    "START_TIME": 1394690400,
    "END_TIME": 1394701199,
    "TEMPERATURE": 299.17,
    "PRESSURE": 1023.2,
    "HUMIDITY": 69,
    "WEATHER_ID": 800,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/schema_detect_str/schema_detect_str.csv",
    "XcalarRankOver": 2
  },
  {
    "ROW_NUMBER": 1,
    "CITY_NAME": "Ajman",
    "CITY_CANONICAL_NAME": "Ajman,Ajman,United Arab Emirates",
    "LAT_LON": "25.41111,55.43504",
    "START_TIME": 1394679600,
    "END_TIME": 1394690399,
    "TEMPERATURE": 297.8,
    "PRESSURE": 1020.66,
    "HUMIDITY": 72,
    "WEATHER_ID": 800,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/schema_detect_str/schema_detect_str.csv",
    "XcalarRankOver": 1
  }
],
"comp" : []
  },
  {
    "name": "test_48",
    "comment": "Handling Xcalar logs as plain text using csv type",
    "sample_file": "/saas-load-test/Error_Handling_Test/absent_delimiters_single_line/absent_delimiters_single_line.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/absent_delimiters_single_line/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "NONE",
        "FieldDelimiter": "~"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "mark": pytest.mark.precheckin,
    "recursive": True,
    "load": [
      {
        "_1": "2020-08-18 17:59:04,323 - Pid 21166 Node 0 Txn 259 - CgroupsAppMgr App - INFO - Received input: {\"func\": \"initCgroups\"}",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/absent_delimiters_single_line/absent_delimiters_single_line.csv"
      },
      {
        "_1": "2020-08-18 17:59:04,361 - Node 0 Cgroup App - INFO - update_cgroup_params: cgroup_name: sys_xpus-sched0.scope, cgroup_controller: memory, cgroup_params: {'memory.limit_in_bytes': 135218340864, 'memory.soft_limit_in_bytes': 121696506777, 'memory.swappiness': 0, 'memory.oom_control': 0}, cg_path: /sys/fs/cgroup/memory/xcalar.slice/xcalar-usrnode.service/sys_xpus-sched0.scope",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/absent_delimiters_single_line/absent_delimiters_single_line.csv"
      }
    ],
    "data": [
      {
        "_1": "2020-08-18 17:59:04,361 - Node 0 Cgroup App - INFO - update_cgroup_params: cgroup_name: sys_xpus-sched0.scope, cgroup_controller: memory, cgroup_params: {'memory.limit_in_bytes': 135218340864, 'memory.soft_limit_in_bytes': 121696506777, 'memory.swappiness': 0, 'memory.oom_control': 0}, cg_path: /sys/fs/cgroup/memory/xcalar.slice/xcalar-usrnode.service/sys_xpus-sched0.scope",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/absent_delimiters_single_line/absent_delimiters_single_line.csv",
        "XcalarRankOver": 2
      },
      {
        "_1": "2020-08-18 17:59:04,323 - Pid 21166 Node 0 Txn 259 - CgroupsAppMgr App - INFO - Received input: {\"func\": \"initCgroups\"}",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/absent_delimiters_single_line/absent_delimiters_single_line.csv",
        "XcalarRankOver": 1
      }
    ],
    "comp": []
  },
  {
    "name": "test_49",
    "comment": "Handle double quote inside a value.",
    "sample_file": "/saas-load-test/Error_Handling_Test/double_quoted_values/double_quoted_values.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/double_quoted_values/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE",
        "QuoteCharacter": "~"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.precheckin,
    "test_type": "load",
    "recursive": True,
    "load": [
      {
        "PHILOSOPHER": "Socrates",
        "QUOTE": "\"The unexamined life is not worth living\"",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/double_quoted_values/double_quoted_values.csv"
      },
      {
        "PHILOSOPHER": "Protagoras",
        "QUOTE": "Man is the measure of all things",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/double_quoted_values/double_quoted_values.csv"
      }
    ],
    "data": [
      {
        "PHILOSOPHER": "Protagoras",
        "QUOTE": "Man is the measure of all things",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/double_quoted_values/double_quoted_values.csv",
        "XcalarRankOver": 2
      },
      {
        "PHILOSOPHER": "Socrates",
        "QUOTE": "\"The unexamined life is not worth living\"",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/double_quoted_values/double_quoted_values.csv",
        "XcalarRankOver": 1
      }
    ],
    "comp": []
  },
  {
    "name": "test_50",
    "comment": "Use *.csv glob to only load .csv from [.csv, .csv.gz]",
    "sample_file": "/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/mixed_types/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.precheckin,
    "test_type": "load",
    "recursive": True,
    "load": [
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 14,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 15,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 16,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 17,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 18,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 19,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 20,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv"
      }
    ],
    "data": [
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 8
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 12
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 4
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 16
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 9
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 7
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 17
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 18
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 2
      },
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 1
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 11
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 3
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 13
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 6
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 10
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 15
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 5
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 14
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 20
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/mixed_types/full_schema.csv",
        "XcalarRankOver": 19
      }
    ],
    "comp": []
  },
  {
    "name": "test_51",
    "sample_file": "/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv",
    "dataset": "/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "recursive": True,
    "load": [
      {
        "A": 1,
        "B": "",
        "C": 3,
        "D": 4,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv"
      },
      {
        "A": 3,
        "B": "",
        "C": 4,
        "D": 6,
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv"
      },
      {
        "A": 2,
        "B": "3",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv"
      }
    ],
    "data": [
      {
        "A": 3,
        "B": "",
        "C": 4,
        "D": 6,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv",
        "XcalarRankOver": 2
      },
      {
        "A": 1,
        "B": "",
        "C": 3,
        "D": 4,
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv",
        "XcalarRankOver": 1
      },
      {
        "A": 2,
        "B": "3",
        "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/csv_fnf_handling/csv_fnf.csv/miss_col.csv",
        "XcalarRankOver": 3
      }
    ],
    "comp": [],
    "mark": pytest.mark.precheckin
  },
  {
    "name": "test_52",
    "comment": "Handling a zero row parquet file",
    "sample_file": "/saas-load-test/Error_Handling_Test/fetch_nonexistent_rows/fetch_nonexistent_rows.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/fetch_nonexistent_rows/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "empty",
    "recursive": False
  },
  {
    "name": "test_53",
    "comment": "Handling parquet missing columns in some rows.",
    "sample_file": "/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "file4.parquet",
    "test_type": "load",
    "recursive": False,
"load" : [
  {
    "N": "d0eca7c1",
    "P": "c4a8b",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "N": "b83a",
    "P": "899b97",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 3,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "P": "0fa0a41",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 4,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "N": "df720",
    "P": "1193d",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 5,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "P": "e8a0",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 6,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "P": "ac3416f8",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 7,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "N": "986451e5",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 8,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "P": "45da1",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 9,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  },
  {
    "P": "07bfab",
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 10,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet"
  }
],
"data" : [
  {
    "N": "986451e5",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 8
  },
  {
    "P": "0fa0a41",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 4
  },
  {
    "P": "45da1",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 9
  },
  {
    "P": "ac3416f8",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 7
  },
  {
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 2
  },
  {
    "N": "d0eca7c1",
    "P": "c4a8b",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 1
  },
  {
    "N": "b83a",
    "P": "899b97",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 3
  },
  {
    "P": "e8a0",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 6
  },
  {
    "P": "07bfab",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 10
  },
  {
    "N": "df720",
    "P": "1193d",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/parquet_with_nulls/file4.parquet",
    "XcalarRankOver": 5
  }
],
"comp" : []
  },
  {
    "name": "test_54",
    "comment": "None value inserted in parquet int64 column",
    "sample_file": "/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet",
    "dataset": "/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "recursive": False,
    "mark": pytest.mark.precheckin,
"load" : [
  {
    "Z": 7649,
    "Q": 5306,
    "S": 2585,
    "C": 5463,
    "E": 867,
    "X": 496,
    "F": 9979,
    "B": 4290,
    "A": 5747,
    "Y": 2662,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet"
  },
  {
    "Z": 6565,
    "Q": 6365,
    "S": 8263,
    "G": 1576,
    "C": 4611,
    "E": 5575,
    "X": 6939,
    "F": 8957,
    "B": 1587,
    "W": 8304,
    "A": 2383,
    "M": 1754,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet"
  },
  {
    "S": 3670,
    "G": 1196,
    "C": 4843,
    "E": 9036,
    "X": 5368,
    "F": 6973,
    "W": 7995,
    "A": 5887,
    "M": 1668,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 3,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet"
  }
],
"data" : [
  {
    "Z": 6565,
    "Q": 6365,
    "S": 8263,
    "G": 1576,
    "C": 4611,
    "E": 5575,
    "X": 6939,
    "F": 8957,
    "B": 1587,
    "W": 8304,
    "A": 2383,
    "M": 1754,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet",
    "XcalarRankOver": 2
  },
  {
    "Z": 7649,
    "Q": 5306,
    "S": 2585,
    "C": 5463,
    "E": 867,
    "X": 496,
    "F": 9979,
    "B": 4290,
    "A": 5747,
    "Y": 2662,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet",
    "XcalarRankOver": 1
  },
  {
    "S": 3670,
    "G": 1196,
    "C": 4843,
    "E": 9036,
    "X": 5368,
    "F": 6973,
    "W": 7995,
    "A": 5887,
    "M": 1668,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/None_in_int64_col/None_in_int64_col.parquet",
    "XcalarRankOver": 3
  }
],
"comp" : []
  },
  {
    "name": "test_55",
    "comment": "Handling nulls in json",
    "sample_file": "/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json",
    "dataset": "/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "null_in_json.json",
    "test_type": "load",
    "recursive": False,
"load" : [
  {
    "A": 2,
    "B": 3,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json"
  },
  {
    "A": 2,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json"
  },
  {
    "A": 2,
    "B": 3,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 3,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json"
  },
  {
    "B": 3,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 4,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json"
  }
],
"data" : [
  {
    "B": 3,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json",
    "XcalarRankOver": 4
  },
  {
    "A": 2,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json",
    "XcalarRankOver": 2
  },
  {
    "A": 2,
    "B": 3,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json",
    "XcalarRankOver": 1
  },
  {
    "A": 2,
    "B": 3,
    "XCALAR_PATH": "/xcfield/saas-load-test/Error_Handling_Test/null_in_json/null_in_json.json",
    "XcalarRankOver": 3
  }
],
"comp" : []
  },
  {
    "name": "test_101",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/100k_100col.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/100k_100col.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "100k_100col.csv.gz",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_102",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/100k_100col.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/100k_100col.csv.bz2",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "100k_100col.csv.bz2",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_103",
    "comment": "ValueError: Columns(1001) must not exceed 1000",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/10k_1000col.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/10k_1000col.csv",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_1000col.csv",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_104",
    "comment": "ValueError: Columns(1001) must not exceed 1000",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/10k_1000col.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/10k_1000col.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_1000col.csv.gz",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_105",
    "comment": "ValueError: Columns(1001) must not exceed 1000",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/10k_1000col.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/10k_1000col.csv.bz2",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_1000col.csv.bz2",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_106",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/10k_100col.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/10k_100col.csv",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_100col.csv",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_107",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/10k_100col.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/10k_100col.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_100col.csv.gz",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_108",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/10k_100col.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/10k_100col.csv.bz2",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_100col.csv.bz2",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_109",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/1m_100col.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/1m_100col.csv",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "1m_100col.csv",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_110",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/1m_100col.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/1m_100col.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "1m_100col.csv.gz",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_111",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/csv/1m_100col.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test/csv/1m_100col.csv.bz2",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "1m_100col.csv.bz2",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_112",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/json/100k_100col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test/json/100k_100col.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "100k_100col.json",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_113",
    "comment": "ValueError: Columns(1001) must not exceed 1000",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/json/10k_1000col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test/json/10k_1000col.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_1000col.json",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_114",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/json/10k_100col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test/json/10k_100col.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "10k_100col.json",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_115",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/json/1m_100col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test/json/1m_100col.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "1m_100col.json",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_116",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/parquet/100k_100col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test/parquet/100k_100col.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 1,
    "file_name_pattern": "100k_100col.parquet",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_117",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/parquet/10k_1000col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test/parquet/10k_1000col.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 1,
    "file_name_pattern": "10k_1000col.parquet",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_118",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/parquet/10k_100col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test/parquet/10k_100col.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 1,
    "file_name_pattern": "10k_100col.parquet",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_119",
    "sample_file": "/saas-load-test/Performance_Scalability_Test/parquet/1m_100col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test/parquet/1m_100col.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 1,
    "file_name_pattern": "1m_100col.parquet",
    "test_type": "global_status",
    "recursive": False,
    "global_status": {
      "error_message": "AWS ERROR: Parquet file is above the max block size"
    },
    "mark": pytest.mark.skip
  },
  {
    "name": "test_120",
    "comment": "Loading 128 large CSV files",
    "sample_file": "/instantdatamart/XcalarCloud/csv/Josh10m-split128/x00.csv",
    "dataset": "/instantdatamart/XcalarCloud/csv/Josh10m-split128/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_121",
    "commment": "Loading 200 tiny json files",
    "sample_file": "/instantdatamart/XcalarCloud/json/tiny200/tiny9.json",
    "dataset": "/instantdatamart/XcalarCloud/json/tiny200/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 1,
    "file_name_pattern": "*.json",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_122",
    "comment": "Loading 2000 tiny json files",
    "sample_file": "/instantdatamart/XcalarCloud/json/tiny2000/tiny1.json",
    "dataset": "/instantdatamart/XcalarCloud/json/tiny2000/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.json",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_123",
    "comment": "Loading 3000 tiny json files",
    "sample_file": "/instantdatamart/XcalarCloud/json/tiny3000/tiny2.json",
    "dataset": "/instantdatamart/XcalarCloud/json/tiny3000/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.json",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.slow
  },
  {
    "name": "test_124",
    "comment": "Loading 50K tiny json files",
    "sample_file": "/instantdatamart/XcalarCloud/json/tiny50000/tiny2.json",
    "dataset": "/instantdatamart/XcalarCloud/json/tiny50000/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.json",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.skip
  },
  {
    "name": "test_125",
    "comment": "Loading 1m csv files",
    "sample_file": "/instantdatamart/csv/taxi_1m_files/x997008.csv",
    "dataset": "/instantdatamart/csv/taxi_1m_files/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_1",
    "sample_file": "/saas-load-test/Real_World_Test/NYC_Parking_Violations_Issued_-_Fiscal_Year_2017.csv",
    "dataset": "/saas-load-test/Real_World_Test/NYC_Parking_Violations_Issued_-_Fiscal_Year_2017.csv",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE",
        "QuoteCharacter": "\"",
        "QuoteEscapeCharacter": "\""
      }
    },
    "num_rows": 4,
    "file_name_pattern": "NYC_Parking_Violations_Issued_-_Fiscal_Year_2017.csv",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_2",
    "sample_file": "/saas-load-test/Real_World_Test/NYC_Parking_Violations_Issued_-_Fiscal_Year_2016.csv.gz",
    "dataset": "/saas-load-test/Real_World_Test/NYC_Parking_Violations_Issued_-_Fiscal_Year_2016.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "NYC_Parking_Violations_Issued_-_Fiscal_Year_2016.csv.gz",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_3",
    "sample_file": "/saas-load-test/Real_World_Test/NYC_Parking_Violations_Issued_-_Fiscal_Year_2015.csv.bz2",
    "dataset": "/saas-load-test/Real_World_Test/NYC_Parking_Violations_Issued_-_Fiscal_Year_2015.csv.bz2",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "NYC_Parking_Violations_Issued_-_Fiscal_Year_2015.csv.bz2",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_4",
    "sample_file": "/saas-load-test/Real_World_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4.csv",
    "dataset": "/saas-load-test/Real_World_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4.csv",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "All_Lending_Club_loan_accepted_2007_to_2018Q4.csv",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_5",
    "sample_file": "/saas-load-test/Real_World_Test/All_Lending_Club_loan_rejected_2007_to_2018Q4.csv.gz",
    "dataset": "/saas-load-test/Real_World_Test/All_Lending_Club_loan_rejected_2007_to_2018Q4.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "All_Lending_Club_loan_rejected_2007_to_2018Q4.csv.gz",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_8",
    "sample_file": "/saas-load-test/Real_World_Test/arxiv_metadata_oai_snapshot.json",
    "dataset": "/saas-load-test/Real_World_Test/arxiv_metadata_oai_snapshot.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "arxiv_metadata_oai_snapshot.json",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_9",
    "sample_file": "/saas-load-test/Real_World_Test/Tweets_during_RM_vs_Liverpool_data.json",
    "dataset": "/saas-load-test/Real_World_Test/Tweets_during_RM_vs_Liverpool_data.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "Tweets_during_RM_vs_Liverpool_data.json",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_10",
    "sample_file": "/saas-load-test/Real_World_Test/epa_air_quality_annual_summary.parquet",
    "dataset": "/saas-load-test/Real_World_Test/epa_air_quality_annual_summary.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 4,
    "file_name_pattern": "epa_air_quality_annual_summary.parquet",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_11",
    "sample_file": "/saas-load-test/Real_World_Test/Seattle_library_collection_inventory_convert.parquet",
    "dataset": "/saas-load-test/Real_World_Test/Seattle_library_collection_inventory_convert.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 4,
    "file_name_pattern": "Seattle_library_collection_inventory_convert.parquet",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "real_12",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4/x15.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 20000,
        "data_rows": 20000,
        "comp_rows": 0
    }
  },
  {
    "name": "real_13",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4_gz/x15.csv.gz",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4_gz/",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv.gz",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 20000,
        "data_rows": 20000,
        "comp_rows": 0
    }
  },
  {
    "name": "real_14",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/NYC_Parking_Violations_Issued_Fiscal_Year_2015_bz2/x19.csv.bz2",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/NYC_Parking_Violations_Issued_Fiscal_Year_2015_bz2/",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv.bz2",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 20000,
        "data_rows": 1732,
        "comp_rows": 18268
    }
  },
  {
    "name": "real_16",
    "comment": "If you want to get 0 complement rows, specify DfString type for REGISTRATION_STATE and HOUSE_NUMBER columns",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/NYC_Parking_Violations_Issued_Fiscal_Year_2017/x17.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/NYC_Parking_Violations_Issued_Fiscal_Year_2017/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 20000,
        "data_rows": 15228,
        "comp_rows": 4772
    }
  },
  {
    "name": "real_17",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/arxiv_metadata_oai_snapshot_converted/xan.json",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/arxiv_metadata_oai_snapshot_converted/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.json",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 20000,
        "data_rows": 20000,
        "comp_rows": 0
    }
  },
  {
    "name": "real_18",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/emergency_911calls/x13.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/emergency_911calls/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 20000,
        "data_rows": 20000,
        "comp_rows": 0
    }
  },
  {
    "name": "real_19",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/customers/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/customers/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 99441,
        "data_rows": 99441,
        "comp_rows": 0
    }
  },
  {
    "name": "real_20",
    "comment": "Seeing intermittent failures: AssertionError: test_case: real_20, assert 989174 == 1000163  +989174 -1000163",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/geolocation/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/geolocation/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 1000163,
        "data_rows": 1000163,
        "comp_rows": 0
    }
  },
  {
    "name": "real_21",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/order_items/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/order_items/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 112650,
        "data_rows": 112650,
        "comp_rows": 0
    }
  },
  {
    "name": "real_22",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/order_payments/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/order_payments/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 103886,
        "data_rows": 103886,
        "comp_rows": 0
    }
  },
  {
    "name": "real_23",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/order_reviews/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/order_reviews/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE",
        "AllowQuotedRecordDelimiter": True
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 99059,
        "data_rows": 99050,
        "comp_rows": 9
    }
  },
  {
    "name": "real_24",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/orders/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/orders/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 99441,
        "data_rows": 99441,
        "comp_rows": 0
    }
  },
  {
    "name": "real_25",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/products/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/products/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 32951,
        "data_rows": 32951,
        "comp_rows": 0
    }
  },
  {
    "name": "real_26",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/sellers/x00.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/sellers/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "*.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 3095,
        "data_rows": 3095,
        "comp_rows": 0
    }
  },
  {
    "name": "real_27",
    "sample_file": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/product_category_name_translation.csv",
    "dataset": "/saas-load-test/Sharing_Dataset_Small_Test/ecommerce/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 4,
    "file_name_pattern": "product_category_name_translation.csv",
    "test_type": "perf",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "result": {
        "load_rows": 71,
        "data_rows": 71,
        "comp_rows": 0
    }
  },
  {
    "name": "accuracy_301",
    "sample_file": "/saas-load-test/Accuracy_Test/csv/full_schema.csv",
    "dataset": "/saas-load-test/Accuracy_Test/csv/full_schema.csv",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "full_schema.csv",
    "test_type": "load",
    "recursive": False,
    "load": [
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 14,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 15,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 16,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 17,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 18,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 19,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 20,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv"
      }
    ],
    "data": [
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 8
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 12
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 4
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 16
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 9
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 7
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 17
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 18
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 2
      },
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 1
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 11
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 3
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 13
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 6
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 10
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 15
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 5
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 14
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 20
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv",
        "XcalarRankOver": 19
      }
    ],
    "comp": [],
    "mark": pytest.mark.slow
  },
  {
    "name": "accuracy_302",
    "sample_file": "/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
    "dataset": "/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "full_schema.csv.gz",
    "test_type": "load",
    "recursive": False,
    "load": [
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 14,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 15,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 16,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 17,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 18,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 19,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 20,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz"
      }
    ],
    "data": [
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 8
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 12
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 4
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 16
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 9
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 7
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 17
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 18
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 2
      },
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 1
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 11
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 3
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 13
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 6
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 10
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 15
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 5
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 14
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 20
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.gz",
        "XcalarRankOver": 19
      }
    ],
    "comp": [],
    "mark": pytest.mark.slow
  },
  {
    "name": "accuracy_303",
    "sample_file": "/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
    "dataset": "/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "full_schema.csv.bz2",
    "test_type": "load",
    "recursive": False,
    "load": [
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 1,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 2,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 3,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 4,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 5,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 6,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 7,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 8,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 9,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 10,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 11,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 12,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 13,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 14,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 15,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 16,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 17,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 18,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 19,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_ICV": "",
        "XCALAR_FILE_RECORD_NUM": 20,
        "XCALAR_SOURCEDATA": "",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2"
      }
    ],
    "data": [
      {
        "FLOAT_COL": "[6.017936542251516e+16]",
        "INT_COL": "[3511285021834768018]",
        "DATETIME_COL": "[Timestamp('2020-02-29 03:40:53')]",
        "STRING_COL": "['kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 8
      },
      {
        "FLOAT_COL": "[2.666338488003091e+18]",
        "INT_COL": "[1886265540154154813]",
        "DATETIME_COL": "[Timestamp('2020-03-21 06:57:17')]",
        "STRING_COL": "['kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 12
      },
      {
        "FLOAT_COL": "[2.547400671179082e+18]",
        "INT_COL": "[8805419045805580330]",
        "DATETIME_COL": "[Timestamp('2020-09-17 06:00:43')]",
        "STRING_COL": "['mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 4
      },
      {
        "FLOAT_COL": "[4.285630016109657e+17]",
        "INT_COL": "[746648936072968903]",
        "DATETIME_COL": "[Timestamp('2020-10-18 05:58:29')]",
        "STRING_COL": "['vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 16
      },
      {
        "FLOAT_COL": "[1.7394449759079665e+18]",
        "INT_COL": "[725922284318568959]",
        "DATETIME_COL": "[Timestamp('2020-09-17 17:43:11')]",
        "STRING_COL": "['ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 9
      },
      {
        "FLOAT_COL": "[6.551767621057284e+18]",
        "INT_COL": "[7692105033234175589]",
        "DATETIME_COL": "[Timestamp('2020-04-11 01:56:22')]",
        "STRING_COL": "['ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 7
      },
      {
        "FLOAT_COL": "[2.8260167217964854e+17]",
        "INT_COL": "[8824812790227047503]",
        "DATETIME_COL": "[Timestamp('2020-04-03 20:59:01')]",
        "STRING_COL": "['zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 17
      },
      {
        "FLOAT_COL": "[5.521416352647821e+18]",
        "INT_COL": "[5530226265881016145]",
        "DATETIME_COL": "[Timestamp('2020-02-09 20:20:51')]",
        "STRING_COL": "['twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 18
      },
      {
        "FLOAT_COL": "[1.3087329126366564e+18]",
        "INT_COL": "[5358165540242065166]",
        "DATETIME_COL": "[Timestamp('2020-04-16 23:01:09')]",
        "STRING_COL": "['imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 2
      },
      {
        "FLOAT_COL": "[3.0884400309052145e+18]",
        "INT_COL": "[224656887126677046]",
        "DATETIME_COL": "[Timestamp('2020-08-26 13:14:46')]",
        "STRING_COL": "['lmbreuqwwpecadtjxf']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 1
      },
      {
        "FLOAT_COL": "[5.289124776710909e+17]",
        "INT_COL": "[3687041929214844627]",
        "DATETIME_COL": "[Timestamp('2020-04-13 22:25:05')]",
        "STRING_COL": "['bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 11
      },
      {
        "FLOAT_COL": "[2.1675633961896918e+17]",
        "INT_COL": "[48757591912307248]",
        "DATETIME_COL": "[Timestamp('2020-09-24 10:24:41')]",
        "STRING_COL": "['zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 3
      },
      {
        "FLOAT_COL": "[1.1512857142433409e+18]",
        "INT_COL": "[6551981463703872942]",
        "DATETIME_COL": "[Timestamp('2020-07-05 18:39:05')]",
        "STRING_COL": "['hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 13
      },
      {
        "FLOAT_COL": "[6.498666163457101e+16]",
        "INT_COL": "[2554298692677638772]",
        "DATETIME_COL": "[Timestamp('2020-02-18 19:24:49')]",
        "STRING_COL": "['zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 6
      },
      {
        "FLOAT_COL": "[1.1980763734877112e+18]",
        "INT_COL": "[8239125623722570939]",
        "DATETIME_COL": "[Timestamp('2020-06-25 07:45:15')]",
        "STRING_COL": "['fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 10
      },
      {
        "FLOAT_COL": "[1.5480588386031593e+18]",
        "INT_COL": "[4643679560604064603]",
        "DATETIME_COL": "[Timestamp('2020-02-15 18:39:27')]",
        "STRING_COL": "['icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 15
      },
      {
        "FLOAT_COL": "[4.480462761804971e+18]",
        "INT_COL": "[6841866305599785990]",
        "DATETIME_COL": "[Timestamp('2020-05-30 00:48:14')]",
        "STRING_COL": "['uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 5
      },
      {
        "FLOAT_COL": "[9.136968649059424e+16]",
        "INT_COL": "[3758187081007619017]",
        "DATETIME_COL": "[Timestamp('2020-07-15 13:57:14')]",
        "STRING_COL": "['prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 14
      },
      {
        "FLOAT_COL": "[9.096500606517016e+17]",
        "INT_COL": "[8926380616685888692]",
        "DATETIME_COL": "[Timestamp('2020-01-16 22:36:08')]",
        "STRING_COL": "['zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb']",
        "BOOLEAN_COL": "[True]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 20
      },
      {
        "FLOAT_COL": "[2.5881588119032102e+17]",
        "INT_COL": "[3754093959192950292]",
        "DATETIME_COL": "[Timestamp('2020-05-07 12:20:18')]",
        "STRING_COL": "['qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz']",
        "BOOLEAN_COL": "[False]",
        "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/csv/full_schema.csv.bz2",
        "XcalarRankOver": 19
      }
    ],
    "comp": [],
    "mark": pytest.mark.slow
  },
  {
    "name": "accuracy_304",
    "comment": "Should load arrays which are now supported!",
    "sample_file": "/saas-load-test/Accuracy_Test/json/full_schema.json",
    "dataset": "/saas-load-test/Accuracy_Test/json/full_schema.json",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "full_schema.json",
    "test_type": "load",
    "recursive": False,
"load" : [
  {
    "FLOAT_COL_0": 3.088440031e+18,
    "INT_COL_0": 224656887126677046,
    "DATETIME_COL_0": 1598447686000,
    "STRING_COL_0": "lmbreuqwwpecadtjxf",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 1,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 1.308732913e+18,
    "INT_COL_0": 5358165540242065166,
    "DATETIME_COL_0": 1587078069000,
    "STRING_COL_0": "imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 2,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 2.167563396e+17,
    "INT_COL_0": 48757591912307248,
    "DATETIME_COL_0": 1600943081000,
    "STRING_COL_0": "zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 3,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 2.547400671e+18,
    "INT_COL_0": 8805419045805580330,
    "DATETIME_COL_0": 1600322443000,
    "STRING_COL_0": "mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 4,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 4.480462762e+18,
    "INT_COL_0": 6841866305599785990,
    "DATETIME_COL_0": 1590799694000,
    "STRING_COL_0": "uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 5,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 6.498666163e+16,
    "INT_COL_0": 2554298692677638772,
    "DATETIME_COL_0": 1582053889000,
    "STRING_COL_0": "zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 6,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 6.551767621e+18,
    "INT_COL_0": 7692105033234175589,
    "DATETIME_COL_0": 1586570182000,
    "STRING_COL_0": "ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 7,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 6.017936542e+16,
    "INT_COL_0": 3511285021834768018,
    "DATETIME_COL_0": 1582947653000,
    "STRING_COL_0": "kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 8,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 1.739444976e+18,
    "INT_COL_0": 725922284318568959,
    "DATETIME_COL_0": 1600364591000,
    "STRING_COL_0": "ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 9,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 1.198076373e+18,
    "INT_COL_0": 8239125623722570939,
    "DATETIME_COL_0": 1593071115000,
    "STRING_COL_0": "fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 10,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 5.289124777e+17,
    "INT_COL_0": 3687041929214844627,
    "DATETIME_COL_0": 1586816705000,
    "STRING_COL_0": "bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 11,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 2.666338488e+18,
    "INT_COL_0": 1886265540154154813,
    "DATETIME_COL_0": 1584773837000,
    "STRING_COL_0": "kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 12,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 1.151285714e+18,
    "INT_COL_0": 6551981463703872942,
    "DATETIME_COL_0": 1593974345000,
    "STRING_COL_0": "hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 13,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 9.136968649e+16,
    "INT_COL_0": 3758187081007619017,
    "DATETIME_COL_0": 1594821434000,
    "STRING_COL_0": "prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 14,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 1.548058839e+18,
    "INT_COL_0": 4643679560604064603,
    "DATETIME_COL_0": 1581791967000,
    "STRING_COL_0": "icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 15,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 4.285630016e+17,
    "INT_COL_0": 746648936072968903,
    "DATETIME_COL_0": 1603000709000,
    "STRING_COL_0": "vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 16,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 2.826016722e+17,
    "INT_COL_0": 8824812790227047503,
    "DATETIME_COL_0": 1585947541000,
    "STRING_COL_0": "zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 17,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 5.521416353e+18,
    "INT_COL_0": 5530226265881016145,
    "DATETIME_COL_0": 1581279651000,
    "STRING_COL_0": "twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 18,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 2.588158812e+17,
    "INT_COL_0": 3754093959192950292,
    "DATETIME_COL_0": 1588854018000,
    "STRING_COL_0": "qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz",
    "BOOLEAN_COL_0": False,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 19,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  },
  {
    "FLOAT_COL_0": 9.096500607e+17,
    "INT_COL_0": 8926380616685888692,
    "DATETIME_COL_0": 1579214168000,
    "STRING_COL_0": "zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb",
    "BOOLEAN_COL_0": True,
    "XCALAR_ICV": "",
    "XCALAR_FILE_RECORD_NUM": 20,
    "XCALAR_SOURCEDATA": "",
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json"
  }
],
"data" : [
  {
    "FLOAT_COL_0": 6.017936542e+16,
    "INT_COL_0": 3511285021834768018,
    "DATETIME_COL_0": 1582947653000,
    "STRING_COL_0": "kolsujturjnfwcmyafdenrnxxeijktnofaaxsdyzcpratjuqhjropboopdaekffthnhbsfhjtxagqfaxpfrvuagggfmwxpojvxmopgfyaqemkvpvktjvypccvweukhjkztjogoprfqgddmnzgsiqpjncrmukibqdnyzrlgaowxxvvusubbvwqrazpimiosvuruhlneheojeyidhjiqikzkvwdhxvnnygizeyuyjqzycaiuzbjfdnlitomjtfwxgolmxndhpkixktwqgjqxaemknhjeocpifeympprdvzpqmhqomxrgsvkymbpezmjzepfsnzbeixldpodghifkrazihqukbkuatgyedbawmlwlfguzlxflaxxfqevsxhtlylneotaxezzlkpoxgnoldqdrscjrctrnbgqumyjbkxppjpisatkjzmwhgoquddpfdqlgmehuxynbmvssuvzgfnjcilbhonrbjvopibwpukewbcfjuqvqadqbtzqlbfwumfxohjizykgckatvhrsboqhayhfngwddotwihxxwnoyyqbjytfrhrptmkysxwcnxcyforsfpfzuxpcyjfujaoyrtcgnvxnoqpdwqfyftaircaflngkqrvxjueeafotjqcnccbyldfkvzxxekxgnizajwjbwkzeffrpduvthijcqegyyyskonbzbffpuxooouwmyvwgnoxsumzggtgaqgvzluihzadymgoivsfuptbpfvaxgqjzajcrryqgnrcrkbkwwxvwsbrfskupewqhzyoijbnmrivnjofyqnsdvykzgmqmeeyilumlidxygrwphnyzsmhylfvhnsigtycfqtjrbgpfenzgrkdcvhiowkjnjyemqiosmsexas",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 8
  },
  {
    "FLOAT_COL_0": 2.666338488e+18,
    "INT_COL_0": 1886265540154154813,
    "DATETIME_COL_0": 1584773837000,
    "STRING_COL_0": "kmbelzgqvllcwqdpiyeusutpclkazxbczzcxhzgqnizselixaokzndobxlueevdmmghnnaixtsjwpsflbzbcpowchebqnjzpxbsvisvrqpzslsgqgmwbmmggukwtaivqywmfukggittpccuaspvrtnzbzmqyemddzkofvrhearekwbwpolkwrjxpsqedygtjoeqblnxufbzqcvqvkuoezgeneirpanwpwqfntrslejypmnvpaizkiospmdjfivehrahxuokfdhshpxkmmjjwvjxqyjjwrwgpmgnzqxodzfrynsxawojpnvvmgtnklhlatiihuydlokrhewrorimfxdnjmtsbbrdziipnvfyqzsovthnyvebmirylpzywhkjvnijpsddrntcxgvldvaqwuakioxqnztelmitwgxcojkxuierlxvvdxywlbzxxfgzuxxbpnuhxndzfbremhvedfmqoboyktfszzuoiliyledopqewpewuqmrqhsyetrrmjmzgieqfmgrbkboqnpkzfvgpagklzatgzyfkrfjfqgmovdzygwpmijsizfiisthbveizscmsfgcuijhjtavdoxymtpuecujwmjustoerdvon",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 12
  },
  {
    "FLOAT_COL_0": 2.547400671e+18,
    "INT_COL_0": 8805419045805580330,
    "DATETIME_COL_0": 1600322443000,
    "STRING_COL_0": "mfdqpcupibuhnvidaldudjpycnpokjfgcuiumlncguomvdmnhatlwxqawzfqxlsqglkmzgliuzybjwchnmmejuklnpkbylfuvkcnwdfuaekzpoabdshjjrwdyvkybhrwsbhiyqhpquldwqpwxdphinfucixabibumwcmcpifmcnqijwmtyfebajlbfvzneboqpqnrjlirpmcaqtiaxsdvnehucjtmcdrvoevsnvloridntstcvtesukxecsdfqsnjcuebkszszxhqlwezsfbedyowcoruuewdxcwlvkbdodlgdbghlkuzjgpglaxbjtobaavokwrayrtymrxtauwnqyfnysnxuhxcebjnarrixwwquzyxnorpohdtmuciaevteneoehdhtljlvaljlenakeswufrfvrjxebawpufnaaqvjxjfwnmdriukvjeigzbxwktakuaomnahrnjdiafomycvnbxwmzmptozulzqakpaukdazkpioopamavwmaqdkrskumwdlphcctbzwzgnmjugdlzlaelnzfcfpskgmqjzetzrhpbycfzmevivbeuufmlqttpwkjrmfmqumigbnhlopybifmrbzltsjhqiuadfegkqonqofvbvfodadlwkxmrdykitajpdrzandtiupgyxxtolqwtbajepgbegalahvacdnaelmucpbklquokizdhoaevymaiqsjrkcaeojvejwfoyngmazhiscthhrovsljgnlmfmlniwjlbjipzgrncrcdthjragmtudofusiaseyfgysmbkvjumpggxbtmowralhsmgftycomrf",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 4
  },
  {
    "FLOAT_COL_0": 4.285630016e+17,
    "INT_COL_0": 746648936072968903,
    "DATETIME_COL_0": 1603000709000,
    "STRING_COL_0": "vzidbwbzlciyrmnnuffinjphaxuqmbmvxivrhbgmripegifqdjbxzeqfnuonflsyjgzzjnkmrqrtoiglgjmbmmqnggwolukrejagwvjaqrxcfcyystliipqqkhpsqskulxobajdaczggftxkhvzrqwqzgtmtjsfzqzbkmbaydkjywjxpblatbntawutjucjywxltb",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 16
  },
  {
    "FLOAT_COL_0": 1.739444976e+18,
    "INT_COL_0": 725922284318568959,
    "DATETIME_COL_0": 1600364591000,
    "STRING_COL_0": "ajgwcucbjralvfvywiglpsllpopugqutjllspizypvrztbgchnlupbciixacewuaojtqhqglbshwsovjdrnpijjklcqdbuowmspbzcwbnnzfvvndfguffsdcdnzjzlbirrojtyztdavxzonynnwamlthihlhicqlqutfwaprirqejljktzxksxyjfzemkfgenvhqievwnbkmvktoxsrjlrisiktijrbezprfbklvjtesflecmumqvupuqnwbbpzmwmyvagpzvkucoxrnzzplgspxftrertgtildexvwamptbusziyafgglxiemuqudsfmhgxyginnnpmzmrfahjfbblakczqttpcnbtaiidxgeaaugadlabvzdubdhhwjdamzpbrvcyvposvzogjjvxdfwuuhghvkshogfocuuvsozszlukrfytmgyafnrycvwarbtmnvtjhruhfibvzcxiecwrgpnjbooeihowgubudfttcmfajbxrjqiqrqxwsocxtcnyzpchiffgimqdfxyhkhzpfensxasjuqnzlcxxmivnavzdwwhekvclbonnrqdsykfzjmcnvtsvsezrxiiblfheiswkmkjjtrytwcxyuoqpmjnqpdycijymvzyxnbmgmekcdafuoyphmdfsptzle",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 9
  },
  {
    "FLOAT_COL_0": 6.551767621e+18,
    "INT_COL_0": 7692105033234175589,
    "DATETIME_COL_0": 1586570182000,
    "STRING_COL_0": "ymgorukqsjbcojyruothyttzmkioqfvbvjwyvckltsthtvzlhqmuazkinwogzkboubnkzkmarfkodnsbuapuhqhnikythoujbrnhnytwebkjmmamcefflgfrmrdlecnzejiylynwucqppumwbcpjgpnhvcleixgjvitxyzoxnltttzcdlzzulninclustorpewszkujvvjvreoxhbpemwdzxwhxgutrlexzkpvzxgdksecztmwtdunbyeorjefqzbntdbvydvcbdjlljmvawyzismsmauddoxsamqghuvtncpzarlcwywdpnqvyegipmkzxfabcbbjedshrkryecywkxpeelmfujjabtywmodttuoryrcuqayompjohslhpdvjknxkvdhirvailzketntmpssrfusdvwshucbpxrmobpeqvkrehztychajwupzvfcmqpxghwxpwgjqxailerlxnudoguiezusishvizwcautbhcywcdlsmsblwwvgvugirgsqtibzeeiktzytby",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 7
  },
  {
    "FLOAT_COL_0": 2.826016722e+17,
    "INT_COL_0": 8824812790227047503,
    "DATETIME_COL_0": 1585947541000,
    "STRING_COL_0": "zpyqnkpkyyrbaaqqnszvqmojjolotpqnssqbemslwnzlsfexfnmrutvjwphbidswbzfztdbztgcjgepytgrfpfzxjkddwlirmmnkdicmjpgnpiygbesqekznmajnxbgjgqrfednbjuwgqovsxqimwoflextynpwnlniqcibkkyowlwjmunnydaaodbbolhpiyykvshuzpihaulhqlwntiqrrqraigbgjvacdqvefciivifsyvn",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 17
  },
  {
    "FLOAT_COL_0": 5.521416353e+18,
    "INT_COL_0": 5530226265881016145,
    "DATETIME_COL_0": 1581279651000,
    "STRING_COL_0": "twezernkpmpdkoofabkjktdtzskaxxhnuyvahmyjmuptjimaxwnhuhcijlgstmllzvmpbwmymvtzfziohtcvjxuihnfutlrecpbhgslinccvphufhcysocggevratvjdidkspaqsmxrrxxjrsmlivugkxcyyyxputsvvsxdhcvubtdekwf",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 18
  },
  {
    "FLOAT_COL_0": 1.308732913e+18,
    "INT_COL_0": 5358165540242065166,
    "DATETIME_COL_0": 1587078069000,
    "STRING_COL_0": "imnppjajqujcphcstmvanntkuqvwctfdhpoumffpoiozzrndwpfctrouqazohwybblqeyswfnpbqhbvctyxwgvyuhybhonncjcderneqfhavybgitfslsjytrekblxtgcenksnniwfgukfbltemhxocfcsrurdeqlpcsyiywyieeofdgzdjjdjrbgwusbntncrdrozehdoxmxtnefjteomjwjkyrryqikasngoaikhzuntkiaukunkuhdiasmfsardxrfdjymjokhjuldcrclqaisowgiqjilmanqgzptuglcwpkrwljxowgniyvitterhcllvidgunszytazjvvdbwcjybfedfgqwoixbbfoosvaguuerbprjpyzwiemzvmknwbiwmyzozqeyiacbdzwyjxkdomiqyekcqxpuuppqivzhmqxepwalcgwlkbosinhoicvypwtydstemoqvvuktvolvhdzmtzhoduttvplzwskjwurshdkzussmtmrfl",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 2
  },
  {
    "FLOAT_COL_0": 3.088440031e+18,
    "INT_COL_0": 224656887126677046,
    "DATETIME_COL_0": 1598447686000,
    "STRING_COL_0": "lmbreuqwwpecadtjxf",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 1
  },
  {
    "FLOAT_COL_0": 5.289124777e+17,
    "INT_COL_0": 3687041929214844627,
    "DATETIME_COL_0": 1586816705000,
    "STRING_COL_0": "bhjoteymaimxvfakrzejvpbgqgnglgrmtoboqeoamphdfnooafipxdwgrioeptruvpwbtajgsjihtteohwunfievpzpoqqrwhqoyzusmmywyisybpjvaqgksqqhwnpvepkigfzdstaasmoepbfyluuywtyzopxuygwibcyqkgklobyawzjwkkpwnsoiabmngqlugxpdzpmwlxmrrishttuzevrdkvhszqcpkjqphyicrwncwlaehljrlzpsajkdxljxymotivnzwhpkxxlipajkydyqimaisxozdbemndpyamhprqbgvyqimefntizugtuppiqmfvtulmyufmaozktrotlruxninbotugdfviskejpumyelfvjwudohvugdwqbubdclwvzgoblvrzjzgkgpsvckidhnrohhhmlsrmodnkutgsqpefqlusqikrrsagaxrneszxiugxzbqiumpqrgfqspkkfvotmgjhlhunhvrypcjhrqiltblrssagmjlqdoiabnbjntboyjgxkxricbbjzsrpzmppegktejxyqdmxrbzurmvbfwzrtgouzuuiejfhugkyktowebzofayeedqmqxbbpbqywupkerwylzckavingzgohqwpxldksnbseyffzzpbhvhdlozydrfadvgcijogncqlsgniqrrwfvojgneutrguzeofuuxrnzkglzbkdzrvsnasubcbhoomvdvzwcdmkuxmkolarificxbpwdkhbxjmozhqgrgcggle",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 11
  },
  {
    "FLOAT_COL_0": 2.167563396e+17,
    "INT_COL_0": 48757591912307248,
    "DATETIME_COL_0": 1600943081000,
    "STRING_COL_0": "zjnseazfgrzatazjlkadoooejjukavgvueuxlohrrcbvrzgysllfaxylxoaixtahwpqjutccvmznuvtxgscafrvcsqrnlihfjeabbpcfbhhmdklthtjimjfymxwplcvhhisjjmbtqkfdwiodfuphwcrmsojricwotxebqnbjiibsmfvarekorteeswihrc",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 3
  },
  {
    "FLOAT_COL_0": 1.151285714e+18,
    "INT_COL_0": 6551981463703872942,
    "DATETIME_COL_0": 1593974345000,
    "STRING_COL_0": "hiuhgxwxfqcqkfjxqwxduahgjvgwxumkjncpqrpuizqkzjsdknxnndufsqaldjexpffknhluzhqgyjwjhbeiagnbtjcawnjcpqupdbtvqjsarqykgnqvqbdkgpcojndqvkgfirti",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 13
  },
  {
    "FLOAT_COL_0": 6.498666163e+16,
    "INT_COL_0": 2554298692677638772,
    "DATETIME_COL_0": 1582053889000,
    "STRING_COL_0": "zsqdtiprcsovhlmmaxnaadpscbmwhotseppiwqqqbixuphzhhmxfijzzrtxqtsxlkphdtvseggzdpulmwvfejhameaancwxeqmrgtemdevinjsyeupliyrhmijqmgksnultivbxhorpibmjicdyjchzjrtyhicvlqcyrechyuoeuywwadkhzwzcmdhlpvjihnyxghupetxvvjqoauesnqsewkpajtjwaydwnbytdfidiibddbudzgttgyqzlxsgvfmnahtvcroqdoszipdxxlnekcistxqylffnxeagoytohlgcoqqldlqunixsrulmflbspqhiqpmrymrxtqlkcththyajuvxkkpvysjgmbjrnmzegitbbuldskuteowrfhmgeuetrbpaetsgzgyrecrdlunhfvarqekemqcidkamijwhvytgntffwtztxslzdwiqsqpbpmubolihelpbzlhgkjiodubygfhpkrjdwwpbcccsmhvvspklqatfpjomsunnzmzpdqeccteofbctgdqxggpjafjkurgktcrynjahofjbagikicby",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 6
  },
  {
    "FLOAT_COL_0": 1.198076373e+18,
    "INT_COL_0": 8239125623722570939,
    "DATETIME_COL_0": 1593071115000,
    "STRING_COL_0": "fqaxxnkopmlcustwsickoowfeufsncstedkhpokvgngonczfuumkxmbvbnsxyomabfvycsgchsqljbkljqgepaxyfyjravucdbikfrzpipfuutgwqkbiy",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 10
  },
  {
    "FLOAT_COL_0": 1.548058839e+18,
    "INT_COL_0": 4643679560604064603,
    "DATETIME_COL_0": 1581791967000,
    "STRING_COL_0": "icqhxcpoxfjytvzijontowlhfqxaftrpobpkcphqjnphailncvwsduwpkjirsuyhfnixvomtuolnbjazhhbszuyinordslvhfrftvldsgmolugckweefpncmxonvtfslztzeajkkfgjxqomnhonviidrymsexyksigkminulmojdnryptwopilympmtqwlldojyualoqancquyhcsfkbxbptdhcjunnrwnyypxvjdhbbnnpsdhbvtmrvbydmaleavgxaibvsrhgwlvuiihpnhjjtrxdnwpptootkcblucrpvmrzreczlrxaopdjpehgzlmjcnsthlsmysykifmextilnyfbrbwkvxkblulfdzcdribscttopjrnwleswmxtxcvfpqofipvwcpxhtkromzwevwmituzrkcoyrnazvourqenmgulghkovovigarynrvbkqochjdxpdzodrqdvqelzaoavpaehtfdhakshcizaqfoftgzlgxwfsqabzwlqdxvtxlemnqqsyiucxwdsnxeihcukvooqwivavbsregnkohzaqdqvycnjyjnmgbgdnrwryeayfsphxgzfwjxfirnnlcmeiqltgtonagncdnqouqwmxjnakncfronzazkjffxxhgjdvyamiwqizaghzmnsgunpqebqeqzbfbqafzgqknxyescgexxrdmeyfhimpyzoihhxkzynmo",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 15
  },
  {
    "FLOAT_COL_0": 4.480462762e+18,
    "INT_COL_0": 6841866305599785990,
    "DATETIME_COL_0": 1590799694000,
    "STRING_COL_0": "uxxnrameygjxvvpkvbxuaslssdmpcytyqwktslexvjaviwpkylmranqicqwmdiqrhvtttaplvhqmjguwbtnbfeehqrelaamicvhhvcqbutkmxjqxmtrjzmhupbhzubjoitwmrsknrimwtogqrumsyquvvybowgbnrvsuolczmsyyerpdrgoytdpunpxghzbzxuahxrfrdepcztwemhrmxgewzedzullpnrrcqsxmrqdbptontdqznrzedzugbgmpkepfeegyhunrgwppcehmbizxnaaezdehebameprjmfnlmrtriilbammniejmwaqkvcitjajgssnghdthqxcjdbxipyzhxxddvkgbmaisjdpqlusfzqbrlafzlnamxmclvxpmbpzxcsugnnngtxecyxgairraqdietcddgiumgavwlqypqrlgnatzaaubyoulyvcgrkomrjmmzbenfqcrtmumefordiuununegbcewnttbqgdeupgtcvldolfelejmxmejazjvumjptotlkpjbhvrqcwzehhnixpqzyhfghovybvdrjejlcojxplktcuzxbkrjudbrbqbiueazjrvjsumzkjywpfsncjhgaohsbhlecbunivmubovulmhytwjpshllnostqilxinzlqzvqlsjfytzingxpgujqugicfjranaufopggtsblsafxtsdpzkowqrtxswlqtogjknibvffzdzlhhiqsxhfmofpalivhdtfaqxlygkhouxtsszjcyualfwbuouwjbtcgibwianikseegvckpvs",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 5
  },
  {
    "FLOAT_COL_0": 9.136968649e+16,
    "INT_COL_0": 3758187081007619017,
    "DATETIME_COL_0": 1594821434000,
    "STRING_COL_0": "prnodjskysvcueodyznbwrjxcqtbbhwqaaenfamilfyyzibowlasrdmgkbjqbfwvblrryanaisrziciyturdxzmjaeraaocrugyrtephzjaqzcojhparqkhrqhophogncjgpevwildctuliyezlmemosqfmkbolrjttdkjztulcavczsnwfonjuafpkvnykxezkadlykeljtbztqbrkueydzgtgourulvpcrsvsxhblumnmclxidxupacevblxnwmeekwifnfriuedmbecntxpdyjbmvhfwvkoeyhqkmutjhjrxqllobcbjvfjmofzcdhmlphloavzkhzpakrteksgqodabuhcotfubftcsiqmisbpuqxjzovsmlefpxyxnltjtvnwxvbpocfabceteheutgploxxsqrtmngdvidxxnnrsvkzvyjbazhsfouekicokzrciwkxczxxozkzsnvwjhqvgcgaybnsulevidcgzpxdgnoxuvwgarcpzcvzbegbwnupjdfijyrfwexpmfoagxdxqviplknbiqkczqqarsikcelmtbpszrzpssadsvsyoyseuzuffsjcxjznxoqowaimqcdpxigyxacldraqzvlmnvjbouxpzpjtxobvjmjryzexoavtmacanflzeudznqbqfgnsaptgckcfstqwtzrisqcjlgfdfuaveilimaaqmpqnbypodbwamasgikcilspnogvawehpsbyexoydkpyrr",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 14
  },
  {
    "FLOAT_COL_0": 9.096500607e+17,
    "INT_COL_0": 8926380616685888692,
    "DATETIME_COL_0": 1579214168000,
    "STRING_COL_0": "zmomjkihrvjvevibkuhruabqufbvfnqahtaujejntzsnkgacbhnnaxcbibqqecojremhykjuhlthztisxkdetswmotkjyhprrjhqiqntjebfkvevefxbkjjfwwwxfmphsozqiiwufnlnetfrqnwuypirrewyjuhqtmzhnxufzxmaodcsyoaheinbjpuczvjivxhedelshisleebvdbvcrbbcefnoxxpfwexrzxxgeogzepwztbgbdhhlvfiaylyteecaepkzfqkzthcjjeuevgkgcmxhjwbeyszxoxufxzjkvduyjwncprthsppncnbvpscbkmnuxcxubygtqavtbvgkzgbyosduusfdvroyhdehabspperdktzauwojhqbmwnekaepalfywmnpflhpophgqbvcmhoduqddowciysqlqkrbhbefvhqcbjnhtdjrsktaotwtkfmojtudpumtriobzptmhkbkmoaitanofrpndpbldkfcddovuypyuvhxtceckjczewrwaimurcwonscqesnyfeerejctjnxuafwsqyxzjpftgqbjfqrdjboemeejhrdqgranhdrlrarnnxhuxbwunwrfpvgdcngozotibjihrrfkjeuxquzbsuuekxbrwzzhhkyapznmqvleqozbowmfjxquetilttbxeqrgdhiwnldvttfwxsrhgzoggnltrclntgrdwncqmnuwhupchnrqahlnywovqymjfgqhnatackdafbqfdmkmsfcamnkrvwkzhlyxvqbafgdppurvrrczbpfuhinwxaxrb",
    "BOOLEAN_COL_0": True,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 20
  },
  {
    "FLOAT_COL_0": 2.588158812e+17,
    "INT_COL_0": 3754093959192950292,
    "DATETIME_COL_0": 1588854018000,
    "STRING_COL_0": "qojwmwsejlltofkziqgnctxqgqxbkbqqcodvhdaeycvmntnkfzmsokyyiydqzcnanodudyctfwtbhfftgmnbbikudnswclkfvxcxnzkalrfebmanptnowzvwdhrdlwcbgcqasjdfzzuwegcnbyewvqpfkmfglkujttwkblxeioznfxkepxwtozrhcxazwxcodrgnmzujsvodeqaqfwmhrttpnsfkqkvwsjvmfwzkfwfuqakgtwvupcnpqgcubphhbaksnsjzkkbycowcngrnllsvjutqofdwaoepxbjjgwhjkjopapleltvedqafgusrdlfibujcznrhyipboanbqpmyoxbnoikbptzlzggqxmtqpzflmoqadzhjvhzqlhsbwnaswyxzycucuidkaewpsqnqspqehaxqoxoknxrkwzmhiipmqqblrjdkqwjlsuanrsgptuvdcvmveokmgjaqdhrahjbvpvqkdaklznwtvuclyxtrgyrqgpuleltivxarczwtrdfwmxwfrbbzcjadrdikkmiirhmzwuziuvgvphysfzsonyistgzmsjdqruhvefqytpdcvzbzlobraeughhdyimkwqpsmlhbgiifszydawghbvyqamnobdmuqtzdglogfjykixorz",
    "BOOLEAN_COL_0": False,
    "XCALAR_PATH": "/xcfield/saas-load-test/Accuracy_Test/json/full_schema.json",
    "XcalarRankOver": 19
  }
],
"comp" : []
  },
  {
    "name": "accuracy_305",
    "comment": "ENG-9788: Should report arrays which are unsupported!",
    "sample_file": "/saas-load-test/Accuracy_Test/parquet/full_schema.parquet",
    "dataset": "/saas-load-test/Accuracy_Test/parquet/full_schema.parquet",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "full_schema.parquet",
    "test_type": "statuses",
    "recursive": False,
    "mark": pytest.mark.xfail,
    "statuses": [
      {
        "unsupported_columns": [
          {
            "name": "FLOAT_COL",
            "mapping": "$.\"FLOAT_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "INT_COL",
            "mapping": "$.\"INT_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "DATETIME_COL",
            "mapping": "$.\"DATETIME_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "STRING_COL",
            "mapping": "$.\"STRING_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "BOOLEAN_COL",
            "mapping": "$.\"BOOLEAN_COL\"[0:]",
            "message": "Arrays are not supported"
          }
        ],
        "error_message": None
      },
      {
        "unsupported_columns": [
          {
            "name": "FLOAT_COL",
            "mapping": "$.\"FLOAT_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "INT_COL",
            "mapping": "$.\"INT_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "DATETIME_COL",
            "mapping": "$.\"DATETIME_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "STRING_COL",
            "mapping": "$.\"STRING_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "BOOLEAN_COL",
            "mapping": "$.\"BOOLEAN_COL\"[0:]",
            "message": "Arrays are not supported"
          }
        ],
        "error_message": None
      },
      {
        "unsupported_columns": [
          {
            "name": "FLOAT_COL",
            "mapping": "$.\"FLOAT_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "INT_COL",
            "mapping": "$.\"INT_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "DATETIME_COL",
            "mapping": "$.\"DATETIME_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "STRING_COL",
            "mapping": "$.\"STRING_COL\"[0:]",
            "message": "Arrays are not supported"
          },
          {
            "name": "BOOLEAN_COL",
            "mapping": "$.\"BOOLEAN_COL\"[0:]",
            "message": "Arrays are not supported"
          }
        ],
        "error_message": None
      }
    ]
  },
  {
    "name": "integrity_350",
    "comment": "Data integrity in larger datasets with 2**10 files",
    "sample_file": "/saas-load-test/Integrity_Tests/csv/file0.csv",
    "dataset": "/saas-load-test/Integrity_Tests/csv/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE",
        "FieldDelimiter": "|",
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "integrity": True,
    "dtype": str,
    "recursive": True,
    "mark": pytest.mark.slow
  },
  {
    "name": "integrity_351",
    "comment": "Data integrity in larger datasets with 2**10 files",
    "sample_file": "/saas-load-test/Integrity_Tests/json/file0.json",
    "dataset": "/saas-load-test/Integrity_Tests/json/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES",
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "integrity": True,
    "dtype": str,
    "recursive": True,
    "mark": pytest.mark.slow
  },
  {
    "name": "integrity_352",
    "comment": "Data integrity in smaller datasets with 2**5 files",
    "sample_file": "/saas-load-test/Integrity_Tests/csv_small/file0.csv",
    "dataset": "/saas-load-test/Integrity_Tests/csv_small/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE",
        "FieldDelimiter": "|",
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "load",
    "integrity": True,
    "dtype": str,
    "recursive": True,
    "mark": pytest.mark.precheckin
  },
  {
    "name": "integrity_353",
    "comment": "Data integrity in smaller datasets with 2**5 files",
    "sample_file": "/saas-load-test/Integrity_Tests/json_small/file0.json",
    "dataset": "/saas-load-test/Integrity_Tests/json_small/",
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.json",
    "test_type": "load",
    "integrity": True,
    "dtype": str,
    "recursive": True,
    "mark": pytest.mark.precheckin
  },
  {
    "name": "integrity_354",
    "comment": "Data integrity in smaller datasets with 2**5 files",
    "sample_file": "/saas-load-test/Integrity_Tests/parquet_small/file0.parquet",
    "dataset": "/saas-load-test/Integrity_Tests/parquet_small/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "integrity": True,
    "dtype": str,
    "recursive": True,
    "mark": pytest.mark.precheckin
  },
  {
    "name": "integrity_355",
    "comment": "Data integrity in smaller datasets with 2**10 files",
    "sample_file": "/saas-load-test/Integrity_Tests/parquet/file0.parquet",
    "dataset": "/saas-load-test/Integrity_Tests/parquet/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "integrity": True,
    "dtype": str,
    "recursive": True,
    "mark": pytest.mark.slow
  },
  {
    "name": "integrity_356",
    "comment": "Data integrity in smaller int datasets with 2**5 files",
    "sample_file": "/saas-load-test/Integrity_Tests/parquet_int/file0.parquet",
    "dataset": "/saas-load-test/Integrity_Tests/parquet_int/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "integrity": True,
    "dtype": int,
    "recursive": True,
    "mark": pytest.mark.precheckin
  },
  {
    "name": "integrity_357",
    "comment": "Data integrity in smaller int datasets with 2**5 files",
    "sample_file": "/saas-load-test/Integrity_Tests/parquet_float/file0.parquet",
    "dataset": "/saas-load-test/Integrity_Tests/parquet_float/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "integrity": True,
    "dtype": float,
    "recursive": True,
    "mark": pytest.mark.precheckin
  },
  {
    "name": "integrity_358",
    "comment": "Data integrity in smaller int datasets with 2**5 files",
    "sample_file": "/saas-load-test/Integrity_Tests/parquet_bool/file0.parquet",
    "dataset": "/saas-load-test/Integrity_Tests/parquet_bool/",
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "load",
    "integrity": True,
    "dtype": bool,
    "recursive": True,
    "mark": pytest.mark.precheckin
  },
  {
    "name": "perf_401",
    "comment": "Loading a subset of the files",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/float_100krow_100col_10000file_9994.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*999*.csv",
    "test_type": "perf",
    "recursive": False,
    "mark": pytest.mark.slow
  },
  {
    "name": "multi_501",
    "sample_file": None,
    "dataset": None,
    "path_prefix": "/xcfield/saas-load-test/LoadMart/datasets/csv/dataset_",
    "file_prefix": "file",
    "count": 100,
    "seed": 0,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "num_rows": 3,
    "file_name_pattern": "*.csv",
    "test_type": "multi",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "multi_502",
    "comment": "LoadMart Parquet Files",
    "sample_file": None,
    "dataset": None,
    "path_prefix": "/xcfield/saas-load-test/LoadMart/datasets/parquet/dataset_",
    "file_prefix": "file",
    "seed": 0,
    "count": 100,
    "input_serialization": {
      "Parquet": {}
    },
    "num_rows": 3,
    "file_name_pattern": "*.parquet",
    "test_type": "multi",
    "recursive": False,
    "mark": pytest.mark.skip
  },
  {
    "name": "oper_550",
    "comment": "Operationalization basic LoadApp test",
    "sample_file": "/export/loadqa/tiny0.csv",
    "dataset": "/export/loadqa/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "NONE",
        "FieldDelimiter": "\t"
      }
    },
    "num_rows": 2,
    "file_name_pattern": "*.csv",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "test_type": "oper",
    "schema": {
  "rowpath": "$",
  "columns": [
    {
      "name": "_1",
      "mapping": "$.\"_1\"",
      "type": "DfInt64"
    },
    {
      "name": "_2",
      "mapping": "$.\"_2\"",
      "type": "DfInt64"
    },
    {
      "name": "_3",
      "mapping": "$.\"_3\"",
      "type": "DfInt64"
    },
    {
      "name": "_4",
      "mapping": "$.\"_4\"",
      "type": "DfInt64"
    },
    {
      "name": "_5",
      "mapping": "$.\"_5\"",
      "type": "DfInt64"
    },
    {
      "name": "_6",
      "mapping": "$.\"_6\"",
      "type": "DfString"
    }
  ]
}
  },
  {
    "name": "oper_551",
    "comment": "Operationalization basic LoadApp test",
    "sample_file": "/export/loadqa/tiny0.csv",
    "dataset": "/export/loadqa/",
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "NONE",
        "FieldDelimiter": "\t"
      }
    },
    "num_rows": 2,
    "file_name_pattern": "*.csv",
    "recursive": True,
    "mark": pytest.mark.precheckin,
    "test_type": "oper",
    "schema": {
  "rowpath": "$",
  "columns": [
    {
      "name": "_1",
      "mapping": "$.\"_1\"",
      "type": "DfString"
    },
    {
      "name": "_2",
      "mapping": "$.\"_2\"",
      "type": "DfString"
    },
    {
      "name": "_3",
      "mapping": "$.\"_3\"",
      "type": "DfString"
    },
    {
      "name": "_4",
      "mapping": "$.\"_4\"",
      "type": "DfString"
    },
    {
      "name": "_5",
      "mapping": "$.\"_5\"",
      "type": "DfString"
    },
    {
      "name": "_6",
      "mapping": "$.\"_6\"",
      "type": "DfString"
    },
    {
      "name": "_7",
      "mapping": "$.\"_7\"",
      "type": "DfString"
    },
    {
      "name": "_8",
      "mapping": "$.\"_8\"",
      "type": "DfString"
    },
    {
      "name": "_9",
      "mapping": "$.\"_9\"",
      "type": "DfString"
    },
    {
      "name": "_10",
      "mapping": "$.\"_10\"",
      "type": "DfString"
    },
    {
      "name": "_11",
      "mapping": "$.\"_11\"",
      "type": "DfString"
    },
    {
      "name": "_12",
      "mapping": "$.\"_12\"",
      "type": "DfString"
    },
    {
      "name": "_13",
      "mapping": "$.\"_13\"",
      "type": "DfString"
    },
    {
      "name": "_14",
      "mapping": "$.\"_14\"",
      "type": "DfString"
    },
    {
      "name": "_15",
      "mapping": "$.\"_15\"",
      "type": "DfString"
    },
    {
      "name": "_16",
      "mapping": "$.\"_16\"",
      "type": "DfString"
    },
    {
      "name": "_17",
      "mapping": "$.\"_17\"",
      "type": "DfString"
    }
  ]
}
}
]
