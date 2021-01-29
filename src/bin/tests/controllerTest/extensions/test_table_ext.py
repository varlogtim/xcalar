import pytest
from IPython.utils.io import capture_output


@pytest.mark.order1
def test_table_list(shell, capsys):
    """check table -list"""

    with capture_output() as captured:
        shell.run_line_magic(magic_name='table', line='-list')

    assert 'ds1' in captured.stdout
    assert 'ds2' in captured.stdout
    assert 'ds_no_stream' in captured.stdout


@pytest.mark.order2
def test_table_list_all(shell, capsys):
    """check table -list_all"""

    with capture_output() as captured:
        shell.run_line_magic(magic_name='table', line='-list_all')

    assert 'ds1' in captured.stdout
    assert 'ds2' in captured.stdout
    assert 'ds_no_stream' in captured.stdout
    assert 'offsets' in captured.stdout
    assert 'info' in captured.stdout
    assert 'all_counts_full_app_delta' in captured.stdout
    assert 'uber' in captured.stdout


@pytest.mark.order3
def test_table_schema_ds1(shell, capsys):
    """check table -schema ds1"""

    with capture_output() as captured:
        shell.run_line_magic(magic_name='table', line='-schema ds1')

    result = """schema: \n +----------+-----------+\n| name     | type      |\n|----------+-----------|\n| col10    | float     |\n| col11    | int       |\n| col12    | int       |\n| col1     | int       |\n| col2     | string    |\n| col3     | string    |\n| col4     | string    |\n| col5     | float     |\n| col6     | float     |\n| col7     | string    |\n| col8     | float     |\n| col9     | int       |\n| kx       | int       |\n| BATCH_ID | int       |\n| ENTRY_DT | timestamp |\n| KX       | int       |\n+----------+-----------+\n\nschemaUpper: \n +----------+-----------+\n| name     | type      |\n|----------+-----------|\n| COL10    | float     |\n| COL11    | int       |\n| COL12    | int       |\n| COL1     | int       |\n| COL2     | string    |\n| COL3     | string    |\n| COL4     | string    |\n| COL5     | float     |\n| COL6     | float     |\n| COL7     | string    |\n| COL8     | float     |\n| COL9     | int       |\n| KX       | int       |\n| BATCH_ID | int       |\n| ENTRY_DT | timestamp |\n+----------+-----------+\n\npks: \n +--------+--------+\n| name   | type   |\n|--------+--------|\n| COL1   | int    |\n| COL2   | string |\n| KX     | int    |\n+--------+--------+"""

    assert result in captured.stdout


@pytest.mark.order4
def test_table_describe(shell, capsys):
    """check table -describe ds1"""

    with capture_output() as captured:
        shell.run_line_magic(magic_name='table', line='-describe ds1')

    result = """{
 "source": {
  "sourceDef": {
   "targetName": "snapshot",
   "type": "dataset",
   "parseArgs": {
    "recordDelim": "\n",
    "fieldDelim": "\t",
    "isCRLF": false,
    "linesToSkip": 1,
    "quoteDelim": "\"",
    "hasHeader": true,
    "schemaFile": "",
    "schemaMode": "loadInput"
   },
   "parserFnName": "default:parseCsv",
   "basePath": "/customer4/",
   "forceCast": false,
   "forceTrim": false,
   "literals": {
    "batch_id": {
     "eval": "getInitBatchId",
     "type": "int"
    },
    "entry_dt": {
     "eval": "getCurrentTimestamp",
     "type": "timestamp"
    },
    "kx": {
     "eval": "getCurrentTimestamp",
     "type": "int"
    }
   }
  },
  "meta": {
   "filter": "eq(1483246800000000,1483246800000000)",
   "path": "ds1base/",
   "path_bkp": "/netstore/solutions/customer4/snapshots/1572225544/ds1/",
   "isFolder": true,
   "sourceDef": "test_base",
   "forceTrim": false,
   "forceCast": false,
   "literals": {
    "BATCH_ID": "def getInitBatchId():\n    return 1\n",
    "ENTRY_DT": "def getCurrentTimestamp():\n    return datetime.now().timestamp() * 1000\n",
    "KX": "def getCurrentTimestamp():\n    return datetime.now().timestamp() * 1000\n"
   }
  }
 },
 "stream": {
  "sourceDef": {
   "targetName": "kafka-base",
   "type": "stream",
   "parserFnName": "parse_kafka_topic",
   "parserTemplateName": "kafka_import_confluent",
   "literals": {
    "entry_dt": {
     "eval": "getCurrentTimestamp",
     "type": "timestamp"
    },
    "kx": {
     "eval": "getCurrentTimestamp",
     "type": "int"
    }
   }
  },
  "meta": {
   "filter": "eq(1,1)",
   "sourceDef": "kafka-base",
   "forceTrim": false,
   "forceCast": false,
   "literals": {
    "ENTRY_DT": "def getCurrentTimestamp():\n    return datetime.now().timestamp() * 1000\n",
    "KX": "def getCurrentTimestamp():\n    return datetime.now().timestamp() * 1000\n"
   }
  }
 },
 "app": null,
 "sink": null
}"""

    assert result in captured.stdout
