import pytest

testlist = [
  {
    "name": "perf_601",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_1000col/float_100krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_602",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_1000col/float_100krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_603",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_1000col/float_100krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_604",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/float_100krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_605",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/float_100krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_606",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/float_100krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_607",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_1000col/float_10krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_608",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_1000col/float_10krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_609",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_1000col/float_10krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_610",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_100col/float_10krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_611",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_100col/float_10krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_612",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_100col/float_10krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_613",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_1000col/float_1mrow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_614",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_1000col/float_1mrow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_615",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_1000col/float_1mrow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_616",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_100col/float_1mrow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_617",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_100col/float_1mrow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_618",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_100col/float_1mrow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_619",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_1000col/integer_100krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_620",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_1000col/integer_100krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_621",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_1000col/integer_100krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_622",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_100col/integer_100krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_623",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_100col/integer_100krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_624",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_100col/integer_100krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_625",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_1000col/integer_10krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_626",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_1000col/integer_10krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_627",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_1000col/integer_10krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_628",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_100col/integer_10krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_629",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_100col/integer_10krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_630",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_100col/integer_10krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_631",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_1000col/integer_1mrow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_632",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_1000col/integer_1mrow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_633",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_1000col/integer_1mrow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_634",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_100col/integer_1mrow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_635",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_100col/integer_1mrow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_636",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_100col/integer_1mrow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_637",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_1000col/mix_100krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_638",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_1000col/mix_100krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_639",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_1000col/mix_100krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_640",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_100col/mix_100krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_641",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_100col/mix_100krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_642",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_100col/mix_100krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_643",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_1000col/mix_10krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_644",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_1000col/mix_10krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_645",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_1000col/mix_10krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_646",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_100col/mix_10krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_647",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_100col/mix_10krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_648",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_100col/mix_10krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_649",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_1000col/mix_1mrow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_650",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_1000col/mix_1mrow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_651",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_1000col/mix_1mrow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_652",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_100col/mix_1mrow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_653",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_100col/mix_1mrow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_654",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_100col/mix_1mrow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_655",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_1000col/sparse_100krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_656",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_1000col/sparse_100krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_657",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_1000col/sparse_100krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_658",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_100col/sparse_100krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_659",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_100col/sparse_100krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_660",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_100col/sparse_100krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_661",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_1000col/sparse_10krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_662",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_1000col/sparse_10krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_663",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_1000col/sparse_10krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_664",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_100col/parse_10krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_665",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_100col/parse_10krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_666",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_100col/parse_10krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_667",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_1000col/sparse_1mrow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_668",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_1000col/sparse_1mrow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_669",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_1000col/sparse_1mrow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_670",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_100col/sparse_1mrow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_671",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_100col/sparse_1mrow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_672",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_100col/sparse_1mrow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_673",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_1000col/string_100krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_674",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_1000col/string_100krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_675",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_1000col/string_100krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_676",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_100col/string_100krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_677",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_100col/string_100krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_678",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_100col/string_100krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_679",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_1000col/string_10krow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_680",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_1000col/string_10krow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_681",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_1000col/string_10krow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_682",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_100col/string_10krow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_683",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_100col/string_10krow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_684",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_100col/string_10krow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_685",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_1000col/string_1mrow_1000col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_686",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_1000col/string_1mrow_1000col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_687",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_1000col/string_1mrow_1000col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_688",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_100col/string_1mrow_100col_10000file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_689",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_100col/string_1mrow_100col_10000file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_690",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_100col/string_1mrow_100col_10000file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/10000_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_691",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_1000col/float_100krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_692",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_1000col/float_100krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_693",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_1000col/float_100krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_694",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_100col/float_100krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_695",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_100col/float_100krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_696",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_100col/float_100krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_697",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_1000col/float_10krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_698",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_1000col/float_10krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_699",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_1000col/float_10krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_700",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_100col/float_10krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_701",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_100col/float_10krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_702",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_100col/float_10krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_703",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_1000col/float_1mrow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_704",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_1000col/float_1mrow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_705",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_1000col/float_1mrow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_706",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_100col/float_1mrow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_707",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_100col/float_1mrow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_708",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_100col/float_1mrow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_709",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_1000col/integer_100krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_710",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_1000col/integer_100krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_711",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_1000col/integer_100krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_712",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_100col/integer_100krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_713",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_100col/integer_100krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_714",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_100col/integer_100krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_715",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_1000col/integer_10krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_716",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_1000col/integer_10krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_717",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_1000col/integer_10krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_718",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_100col/integer_10krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_719",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_100col/integer_10krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_720",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_100col/integer_10krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_721",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_1000col/integer_1mrow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_722",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_1000col/integer_1mrow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_723",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_1000col/integer_1mrow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_724",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_100col/integer_1mrow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_725",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_100col/integer_1mrow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_726",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_100col/integer_1mrow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_727",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_1000col/mix_100krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_728",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_1000col/mix_100krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_729",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_1000col/mix_100krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_730",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_100col/mix_100krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_731",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_100col/mix_100krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_732",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_100col/mix_100krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_733",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_1000col/mix_10krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_734",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_1000col/mix_10krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_735",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_1000col/mix_10krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_736",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_100col/mix_10krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_737",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_100col/mix_10krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_738",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_100col/mix_10krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_739",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_1000col/mix_1mrow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_740",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_1000col/mix_1mrow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_741",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_1000col/mix_1mrow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_742",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_100col/mix_1mrow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_743",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_100col/mix_1mrow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_744",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_100col/mix_1mrow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_745",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_1000col/sparse_100krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_746",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_1000col/sparse_100krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_747",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_1000col/sparse_100krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_748",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_100col/parse_100krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_749",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_100col/parse_100krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_750",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_100col/parse_100krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_751",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_1000col/sparse_10krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_752",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_1000col/sparse_10krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_753",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_1000col/sparse_10krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_754",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_100col/parse_10krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_755",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_100col/parse_10krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_756",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_100col/parse_10krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_757",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_1000col/sparse_1mrow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_758",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_1000col/sparse_1mrow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_759",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_1000col/sparse_1mrow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_760",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_100col/parse_1mrow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_761",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_100col/parse_1mrow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_762",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_100col/parse_1mrow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_763",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_1000col/string_100krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_764",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_1000col/string_100krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_765",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_1000col/string_100krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_766",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_100col/string_100krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_767",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_100col/string_100krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_768",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_100col/string_100krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_769",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_1000col/string_10krow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_770",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_1000col/string_10krow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_771",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_1000col/string_10krow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_772",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_100col/string_10krow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_773",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_100col/string_10krow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_774",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_100col/string_10krow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_775",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_1000col/string_1mrow_1000col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_776",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_1000col/string_1mrow_1000col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_777",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_1000col/string_1mrow_1000col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_778",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_100col/string_1mrow_100col_100file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_779",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_100col/string_1mrow_100col_100file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_780",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_100col/string_1mrow_100col_100file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/100_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_781",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_1000col/float_100krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_782",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_1000col/float_100krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_783",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_1000col/float_100krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_784",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_100col/float_100krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_785",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_100col/float_100krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_786",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_100col/float_100krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_787",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_1000col/float_10krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_788",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_1000col/float_10krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_789",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_1000col/float_10krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_790",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_100col/float_10krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_791",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_100col/float_10krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_792",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_100col/float_10krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_793",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/1mrow_100col/float_1mrow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_794",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/1mrow_100col/float_1mrow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_795",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/1mrow_100col/float_1mrow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_796",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_1000col/integer_100krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_797",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_1000col/integer_100krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_798",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_1000col/integer_100krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_799",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_100col/integer_100krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_800",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_100col/integer_100krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_801",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_100col/integer_100krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_802",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_1000col/integer_10krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_803",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_1000col/integer_10krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_804",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_1000col/integer_10krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_805",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_100col/integer_10krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_806",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_100col/integer_10krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_807",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_100col/integer_10krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_808",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/1mrow_100col/integer_1mrow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_809",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/1mrow_100col/integer_1mrow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_810",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/1mrow_100col/integer_1mrow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_811",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_1000col/mix_100krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_812",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_1000col/mix_100krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_813",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_1000col/mix_100krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_814",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_100col/mix_100krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_815",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_100col/mix_100krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_816",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_100col/mix_100krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_817",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_1000col/mix_10krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_818",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_1000col/mix_10krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_819",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_1000col/mix_10krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_820",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_100col/mix_10krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_821",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_100col/mix_10krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_822",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_100col/mix_10krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_823",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/1mrow_100col/mix_1mrow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_824",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/1mrow_100col/mix_1mrow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_825",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/1mrow_100col/mix_1mrow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_826",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_1000col/sparse_100krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_827",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_1000col/sparse_100krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_828",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_1000col/sparse_100krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_829",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_100col/parse_100krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_830",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_100col/parse_100krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_831",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_100col/parse_100krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_832",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_1000col/sparse_10krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_833",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_1000col/sparse_10krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_834",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_1000col/sparse_10krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_835",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_100col/parse_10krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_836",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_100col/parse_10krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_837",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_100col/parse_10krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_838",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/1mrow_100col/parse_1mrow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_839",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/1mrow_100col/parse_1mrow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_840",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/1mrow_100col/parse_1mrow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_841",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_1000col/string_100krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_842",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_1000col/string_100krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_843",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_1000col/string_100krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_844",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_100col/string_100krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_845",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_100col/string_100krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_846",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_100col/string_100krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_847",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_1000col/string_10krow_1000col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_848",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_1000col/string_10krow_1000col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_849",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_1000col/string_10krow_1000col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_850",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_100col/string_10krow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_851",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_100col/string_10krow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_852",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_100col/string_10krow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_853",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/1mrow_100col/string_1mrow_100col_1file_0.csv",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.csv",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_854",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/1mrow_100col/string_1mrow_100col_1file_0.csv.gz",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "GZIP",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.gz",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_855",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/1mrow_100col/string_1mrow_100col_1file_0.csv.bz2",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/csv/one_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "CompressionType": "BZ2",
      "CSV": {
        "FileHeaderInfo": "USE"
      }
    },
    "file_name_pattern": "*.bz2",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_856",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/100krow_1000col/boolean_100krow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_857",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/100krow_100col/boolean_100krow_100col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_858",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/10krow_1000col/boolean_10krow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_859",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/10krow_100col/boolean_10krow_100col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_860",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/1mrow_1000col/boolean_1mrow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_861",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/1mrow_100col/boolean_1mrow_100col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/boolean/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_862",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/100krow_1000col/float_100krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_863",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/100krow_100col/float_100krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_864",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/10krow_1000col/float_10krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_865",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/10krow_100col/float_10krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_866",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/1mrow_1000col/float_1mrow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_867",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/1mrow_100col/float_1mrow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_868",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/100krow_1000col/integer_100krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_869",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/100krow_100col/integer_100krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_870",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/10krow_1000col/integer_10krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_871",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/10krow_100col/integer_10krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_872",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/1mrow_1000col/integer_1mrow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_873",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/1mrow_100col/integer_1mrow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_874",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/100krow_1000col/mix_100krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_875",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/100krow_100col/mix_100krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_876",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/10krow_1000col/mix_10krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_877",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/10krow_100col/mix_10krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_878",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/1mrow_1000col/mix_1mrow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_879",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/1mrow_100col/mix_1mrow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_880",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/100krow_1000col/sparse_100krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_881",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/100krow_100col/sparse_100krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_882",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/10krow_1000col/sparse_10krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_883",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/10krow_100col/parse_10krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_884",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/1mrow_1000col/sparse_1mrow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_885",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/1mrow_100col/sparse_1mrow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_886",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/100krow_1000col/string_100krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_887",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/100krow_100col/string_100krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_888",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/10krow_1000col/string_10krow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_889",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/10krow_100col/string_10krow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_890",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/1mrow_1000col/string_1mrow_1000col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_891",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/1mrow_100col/string_1mrow_100col_10000file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/10000_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_892",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/100krow_1000col/boolean_100krow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_893",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/100krow_100col/boolean_100krow_100col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_894",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/10krow_1000col/boolean_10krow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_895",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/10krow_100col/boolean_10krow_100col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_896",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/1mrow_1000col/boolean_1mrow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_897",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/1mrow_100col/boolean_1mrow_100col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/boolean/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_898",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/100krow_1000col/float_100krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_899",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/100krow_100col/float_100krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_900",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/10krow_1000col/float_10krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_901",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/10krow_100col/float_10krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_902",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/1mrow_1000col/float_1mrow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_903",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/1mrow_100col/float_1mrow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_904",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/100krow_1000col/integer_100krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_905",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/100krow_100col/integer_100krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_906",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/10krow_1000col/integer_10krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_907",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/10krow_100col/integer_10krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_908",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/1mrow_1000col/integer_1mrow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_909",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/1mrow_100col/integer_1mrow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_910",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/100krow_1000col/mix_100krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_911",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/100krow_100col/mix_100krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_912",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/10krow_1000col/mix_10krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_913",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/10krow_100col/mix_10krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_914",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/1mrow_1000col/mix_1mrow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_915",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/1mrow_100col/mix_1mrow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_916",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/100krow_1000col/sparse_100krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_917",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/100krow_100col/parse_100krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_918",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/10krow_1000col/sparse_10krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_919",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/10krow_100col/parse_10krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_920",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/1mrow_1000col/sparse_1mrow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_921",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/1mrow_100col/parse_1mrow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_922",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/100krow_1000col/string_100krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_923",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/100krow_100col/string_100krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_924",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/10krow_1000col/string_10krow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_925",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/10krow_100col/string_10krow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_926",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/1mrow_1000col/string_1mrow_1000col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_927",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/1mrow_100col/string_1mrow_100col_100file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/100_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_928",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/100krow_1000col/boolean_100krow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_929",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/100krow_100col/boolean_100krow_100col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_930",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/10krow_1000col/boolean_10krow_1000col_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_931",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/10krow_100col/boolean_10krow_100col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_932",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/1mrow_100col/boolean_1mrow_100col.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/boolean/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_933",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/100krow_1000col/float_100krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_934",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/100krow_100col/float_100krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_935",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/10krow_1000col/float_10krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_936",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/10krow_100col/float_10krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_937",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/1mrow_100col/float_1mrow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_938",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/100krow_1000col/integer_100krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_939",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/100krow_100col/integer_100krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_940",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/10krow_1000col/integer_10krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_941",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/10krow_100col/integer_10krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_942",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/1mrow_100col/integer_1mrow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_943",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/100krow_1000col/mix_100krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_944",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/100krow_100col/mix_100krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_945",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/10krow_1000col/mix_10krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_946",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/10krow_100col/mix_10krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_947",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/1mrow_100col/mix_1mrow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_948",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/100krow_1000col/sparse_100krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_949",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/100krow_100col/parse_100krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_950",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/10krow_1000col/sparse_10krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_951",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/10krow_100col/parse_10krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_952",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/1mrow_100col/parse_1mrow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_953",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/100krow_1000col/string_100krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_954",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/100krow_100col/string_100krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_955",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/10krow_1000col/string_10krow_1000col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_956",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/10krow_100col/string_10krow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_957",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/1mrow_100col/string_1mrow_100col_1file_0.json",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/json/one_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "JSON": {
        "Type": "LINES"
      }
    },
    "file_name_pattern": "*.json",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_958",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/100krow_1000col/boolean_100krow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_959",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/100krow_100col/boolean_100krow_100col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_960",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/10krow_1000col/boolean_10krow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_961",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/10krow_100col/boolean_10krow_100col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_962",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/1mrow_1000col/boolean_1mrow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_963",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/1mrow_100col/boolean_1mrow_100col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/boolean/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_964",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/100krow_1000col/float_100krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_965",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/100krow_100col/float_100krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_966",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/10krow_1000col/float_10krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_967",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/10krow_100col/float_10krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_968",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/1mrow_1000col/float_1mrow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_969",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/1mrow_100col/float_1mrow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_970",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/100krow_1000col/integer_100krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_971",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/100krow_100col/integer_100krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_972",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/10krow_1000col/integer_10krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_973",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/10krow_100col/integer_10krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_974",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/1mrow_1000col/integer_1mrow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_975",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/1mrow_100col/integer_1mrow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_976",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/100krow_1000col/mix_100krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_977",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/100krow_100col/mix_100krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_978",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/10krow_1000col/mix_10krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_979",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/10krow_100col/mix_10krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_980",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/1mrow_1000col/mix_1mrow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_981",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/1mrow_100col/mix_1mrow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_982",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/100krow_1000col/sparse_100krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_983",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/100krow_100col/sparse_100krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_984",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/10krow_1000col/sparse_10krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_985",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/10krow_100col/parse_10krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_986",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/1mrow_1000col/sparse_1mrow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_987",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/1mrow_100col/sparse_1mrow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_988",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/100krow_1000col/string_100krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_989",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/100krow_100col/string_100krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_990",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/10krow_1000col/string_10krow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_991",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/10krow_100col/string_10krow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_992",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/1mrow_1000col/string_1mrow_1000col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_993",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/1mrow_100col/string_1mrow_100col_10000file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/10000_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_994",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/100krow_1000col/boolean_100krow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_995",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/100krow_100col/boolean_100krow_100col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_996",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/10krow_1000col/boolean_10krow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_997",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/10krow_100col/boolean_10krow_100col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_998",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/1mrow_1000col/boolean_1mrow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_999",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/1mrow_100col/boolean_1mrow_100col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/boolean/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1000",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/100krow_1000col/float_100krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1001",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/100krow_100col/float_100krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1002",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/10krow_1000col/float_10krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1003",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/10krow_100col/float_10krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1004",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/1mrow_1000col/float_1mrow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1005",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/1mrow_100col/float_1mrow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1006",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/100krow_1000col/integer_100krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1007",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/100krow_100col/integer_100krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1008",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/10krow_1000col/integer_10krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1009",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/10krow_100col/integer_10krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1010",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/1mrow_1000col/integer_1mrow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1011",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/1mrow_100col/integer_1mrow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1012",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/100krow_1000col/mix_100krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1013",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/100krow_100col/mix_100krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1014",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/10krow_1000col/mix_10krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1015",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/10krow_100col/mix_10krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1016",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/1mrow_1000col/mix_1mrow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1017",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/1mrow_100col/mix_1mrow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1018",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/100krow_1000col/sparse_100krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1019",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/100krow_100col/parse_100krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1020",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/10krow_1000col/sparse_10krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1021",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/10krow_100col/parse_10krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1022",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/1mrow_1000col/sparse_1mrow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1023",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/1mrow_100col/parse_1mrow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1024",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/100krow_1000col/string_100krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1025",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/100krow_100col/string_100krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1026",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/10krow_1000col/string_10krow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1027",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/10krow_100col/string_10krow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1028",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/1mrow_1000col/string_1mrow_1000col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/1mrow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1029",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/1mrow_100col/string_1mrow_100col_100file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/100_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1030",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/100krow_1000col/boolean_100krow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1031",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/100krow_100col/boolean_100krow_100col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1032",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/10krow_1000col/boolean_10krow_1000col_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1033",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/10krow_100col/boolean_10krow_100col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1034",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/1mrow_100col/boolean_1mrow_100col.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/boolean/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1035",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/100krow_1000col/float_100krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1036",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/100krow_100col/float_100krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1037",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/10krow_1000col/float_10krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1038",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/10krow_100col/float_10krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1039",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/1mrow_100col/float_1mrow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/float/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1040",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/100krow_1000col/integer_100krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1041",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/100krow_100col/integer_100krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1042",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/10krow_1000col/integer_10krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1043",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/10krow_100col/integer_10krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1044",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/1mrow_100col/integer_1mrow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/integer/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1045",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/100krow_1000col/mix_100krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1046",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/100krow_100col/mix_100krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1047",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/10krow_1000col/mix_10krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1048",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/10krow_100col/mix_10krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1049",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/1mrow_100col/mix_1mrow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/mix/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1050",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/100krow_1000col/sparse_100krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1051",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/100krow_100col/parse_100krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1052",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/10krow_1000col/sparse_10krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1053",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/10krow_100col/parse_10krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1054",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/1mrow_100col/parse_1mrow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/parse/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1055",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/100krow_1000col/string_100krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/100krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1056",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/100krow_100col/string_100krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/100krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1057",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/10krow_1000col/string_10krow_1000col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/10krow_1000col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.slow
  },
  {
    "name": "perf_1058",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/10krow_100col/string_10krow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/10krow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  },
  {
    "name": "perf_1059",
    "sample_file": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/1mrow_100col/string_1mrow_100col_1file_0.parquet",
    "dataset": "/saas-load-test/Performance_Scalability_Test_Multiple/parquet/one_file/string/1mrow_100col/",
    "test_type": "perf",
    "num_rows": 2,
    "recursive": False,
    "input_serialization": {
      "Parquet": {}
    },
    "file_name_pattern": "*.parquet",
    "mark": pytest.mark.skip
  }
]
