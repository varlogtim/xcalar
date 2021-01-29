# Test config file for GCE system test - 52GB per node, 16 nodes

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
            'Converted most of the customer5 POC into a list of cli commands',
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
        'UploadRetinaUdfMaxRetry':
            60,
        'UploadRetinaRetryTimeoutSec':
            5,
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
                    False,    # Turn this on once we figure out how to make load from GCS faster
                'dstTable':
                    "acpchanges-per-sysid",
                'retinaParams': {
                    'pathToCustomer2Datasets': 'nfs:///xcalar-qa/datasets/customer2'
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
                'Enabled': False,    # Turn this on once gs connector works
                'dstTable': "EndResult-Customer9",
                'retinaParams': {
                    'pathToCustomer9Datasets': 'gs://xcqa/datasets/earningscap'
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
            }
        ]
    },
}
