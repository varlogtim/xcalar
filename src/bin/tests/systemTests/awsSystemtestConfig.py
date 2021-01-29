# Test config file for AWS system test

testCaseConfig = {
    'filter test': {
        'Enabled': True,
        'Description': 'test filter',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '100MB',
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'Basic Test': {
        'Enabled': True,
        'Description': 'Basic test to illustrate how to add a new test case.',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '2GB',
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'flight demo test': {
        'Enabled': True,
        'Description': 'test flight demo',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'Multi join test': {
        'Enabled': True,
        'Description': 'test multi join',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'Customer5 Test': {
        'Enabled':
            False,
        'Description':
            'Converted most of the Customer5 POC into a list of cli commands',
        'ExpectedRunTime':
            100000,    # In seconds for 1 run
        'NumRuns':
            1,    # Number of times a given user will invoke main()
        'SingleThreaded':
            False,
        'ExclusiveSession':
            False,
    },
    'XcalarSim': {
        'Enabled': False,
        'Description': 'Use Xcalar Sim to run random queries',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 10,    # Number of times a given user will invoke main()
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'Simple Join Test': {
        'Enabled': True,
        'Description': 'test the different types of joins',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '2GB',
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'replay session test': {
        'Enabled': False,
        'Description': 'replay session test',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'SingleThreaded': True,
        'ExclusiveSession': True,
    },
    'Query Regression Test Suite': {
        'Enabled':
            True,
        'Description':
            'Query Regression Test Suite',
        'ExpectedRunTime':
            600,    # In seconds for 1 run
        'NumRuns':
            1,    # Number of times a given user will invoke main()
        'SingleThreaded':
            True,
        'ExclusiveSession':
            False,
        'TransientFailuresAllowed':
            False,
        'retinas': [
            {
                'name': 'Customer13Lrq',
                'path': "BUILD_DIR/src/data/qa/customer13.tar.gz",
                'Enabled': True,
                'dstTable': "result-uC719",
                'retinaParams': {
                    'pathToQaDatasets': '/netstore/datasets',
                },
                'resultNumRows': 13,
            },
            {
                'name':
                    'Customer12Lrq',
                'path':
                    "BUILD_DIR/src/data/qa/retinaTests/customer12Batch.tar.gz",
                'Enabled':
                    True,
                'dstTable':
                    "maxVolCustomer12-Ay51",
                'retinaParams': {
                    'pathToQaDatasets': '/netstore/datasets',
                    'tablePrefix': 'customer12Batch',
                    'datasetName': 'customer12Batch',
                },
                'expectedAnswer': [{
                    u'Stock_max': u'573576400.0',
                    u'YMD': u'20120518',
                    u'Stock': u'FB'
                },
                                   {
                                       u'Stock_max': u'431332600.0',
                                       u'YMD': u'20151117',
                                       u'Stock': u'GE'
                                   },
                                   {
                                       u'Stock_max': u'100988800.0',
                                       u'YMD': u'20161110',
                                       u'Stock': u'WFC'
                                   },
                                   {
                                       u'Stock_max': u'100988800.0',
                                       u'YMD': u'20161110',
                                       u'Stock': u'WFC'
                                   },
                                   {
                                       u'Stock_max': u'217294200.0',
                                       u'YMD': u'20120511',
                                       u'Stock': u'JPM'
                                   }]
            },
            {
                'name':
                    'Customer2BatchAcp',
                'path':
                    "BUILD_DIR/src/data/qa/customer2/batch-acp.tar.gz",
                'Enabled':
                    False,    # Turn this on after the datasets are on s3
                'dstTable':
                    "acpchanges-per-sysid",
                'retinaParams': {
                    'pathToCustomer2Datasets': 's3://xcalar-qa/datasets/Customer2'
                },
                'expectedAnswer': [{
                    u'sysid': u'20110111786',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110115799',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110113203',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20111115221',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110112033',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110111561',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110116873',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110114201',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110113922',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110116963',
                    u'numAcpChanges': u'1'
                }, {
                    u'sysid': u'20110113669',
                    u'numAcpChanges': u'1'
                }]
            },
            {
                'name': 'LGLrq',
                'path': "BUILD_DIR/src/data/qa/LgDfg.tar.gz",
                'Enabled': True,
                'dstTable': "EndResult-LG",
                'retinaParams': {
                    'LgLrqSessionDataset':
                        '/netstore/datasets/lg_generated/sessions/'
                },
                'resultNumRowGMT': 8489,
                'resultNumRowPDT': 9450,
                'resultNumRowUTC': 9686
            },
            {
                'name': 'Customer9Lrq',
                'path': "BUILD_DIR/src/data/qa/EarningsToCapFull.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable': "EndResult-Customer9",
                'retinaParams': {
                    'pathToCustomer9Datasets': 's3://xcfield/datasets/earningscap'
                },
                'resultNumRows': 243480147,
            },
            {
                'name':
                    'MatrixGenMultiplyLrq',    # No verification done for this retina as the results depend on the number of cluster nodes
                'path': "BUILD_DIR/src/data/qa/MatrixGenMultiply.tar.gz",
                'Enabled': True,
                'dstTable': "EndResult-Matrix",
                'retinaParams': {
                    'pathToMatrixDatasets':
                        'nfs:///netstore/datasets/matrixMultiply/partdata/'
                },
            },
            {
                'name':
                    'tpchq1',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq1.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq1",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query1_result/',
                'resultNumRows':
                    4,
            },
            {
                'name':
                    'tpchq2',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq2.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq2",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query2_result/',
                'resultNumRows':
                    100,
            },
            {
                'name':
                    'tpchq3',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq3.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq3",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query3_result/',
                'resultNumRows':
                    11620,
            },
            {
                'name':
                    'tpchq4',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq4.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq4",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query4_result/',
                'resultNumRows':
                    5,
            },
            {
                'name':
                    'tpchq5',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq5.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq5",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query5_result/',
                'resultNumRows':
                    5,
            },
            {
                'name':
                    'tpchq6',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq6.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq6",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query6_result/',
                'resultNumRows':
                    1,
            },
            {
                'name':
                    'tpchq7',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq7.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq7",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query7_result/',
                'resultNumRows':
                    4,
            },
            {
                'name':
                    'tpchq8',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq8.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq8",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query8_result/',
                'resultNumRows':
                    2,
            },
            {
                'name':
                    'tpchq9',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq9.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq9",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query9_result/',
                'resultNumRows':
                    175,
            },
            {
                'name':
                    'tpchq10',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq10.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq10",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query10_result/',
                'resultNumRows':
                    37967,
            },
            {
                'name':
                    'tpchq11',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq11.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq11",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query11_result/',
                'resultNumRows':
                    1048,
            },
            {
                'name':
                    'tpchq12',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq12.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq12",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query12_result/',
                'resultNumRows':
                    2,
            },
            {
                'name':
                    'tpchq13',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq13.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq13",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query13_result/',
                'resultNumRows':
                    42,
            },
            {
                'name':
                    'tpchq14',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq14.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq14",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query14_result/',
                'resultNumRows':
                    1,
            },
            {
                'name':
                    'tpchq15',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq15.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq15",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query15_result/',
                'resultNumRows':
                    1,
            },
            {
                'name':
                    'tpchq16',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq16.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq16",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query16_result/',
                'resultNumRows':
                    18314,
            },
            {
                'name':
                    'tpchq17',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq17.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq17",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query17_result/',
                'resultNumRows':
                    1,
            },
            {
                'name':
                    'tpchq18',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq18.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq18",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query18_result/',
                'resultNumRows':
                    57,
            },
            {
                'name':
                    'tpchq19',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq19.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq19",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query19_result/',
                'resultNumRows':
                    1,
            },
            {
                'name':
                    'tpchq20',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq20.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq20",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query20_result/',
                'resultNumRows':
                    263,
            },
            {
                'name':
                    'tpchq21',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq21.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq21",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query21_result/',
                'resultNumRows':
                    411,
            },
            {
                'name':
                    'tpchq22',
                'path':
                    "BUILD_DIR/src/data/qa/tpch/tpchq22.tar.gz",
                'Enabled':
                    False,    # Turn this on after converting this to 1.3 world
                'dstTable':
                    "EndResult-tpchq22",
                'retinaParams': {
                    'pathToTpchDatasets': 's3://xcfield/benchmarks/tpch_sf1'
                },
                'resultsDatasetPath':
                    's3://xcfield/benchmarks/tpch_sf1_results/query22_result/',
                'resultNumRows':
                    7,
            }
        ]
    },
}
