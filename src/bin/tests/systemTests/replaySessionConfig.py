testCaseConfig = {
    'filter test': {
        'Enabled': False,
        'Description': 'test filter',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '100MB',
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'Basic Test': {
        'Enabled': False,
        'Description': 'Basic test to illustrate how to add a new test case.',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '2GB',
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'flight demo test': {
        'Enabled': False,
        'Description': 'test flight demo',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'Multi join test': {
        'Enabled': False,
        'Description': 'test multi join',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },

    # This test is disabled as the data is no longer available on /netstore
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
        'Enabled': False,
        'Description': 'test the different types of joins',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '2GB',
        'SingleThreaded': False,
        'ExclusiveSession': False,
    },
    'replay session test': {
        'Enabled': True,
        'Description': 'replay session test',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'SingleThreaded': False,
        'ExclusiveSession': True,
    },
    'Query Regression Test Suite': {
        'Enabled':
            False,
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
                    'pathToQaDatasets': '/freenas/datasets',
                    'tablePrefix': 'customer12Batch',
                    'datasetName': 'customer12Batch',
                },
                'expectedAnswer': [{
                    'Stock': 'FB',
                    'Stock_max': '573576400.0',
                    'YMD': '20120518'
                },
                                   {
                                       'Stock': 'XOM',
                                       'Stock_max': '44656400.0',
                                       'YMD': '20170330'
                                   },
                                   {
                                       'Stock': 'XOM',
                                       'Stock_max': '44656400.0',
                                       'YMD': '20170330'
                                   },
                                   {
                                       'Stock': 'XOM',
                                       'Stock_max': '44656400.0',
                                       'YMD': '20170330'
                                   },
                                   {
                                       'Stock': 'XOM',
                                       'Stock_max': '44656400.0',
                                       'YMD': '20170330'
                                   },
                                   {
                                       'Stock': 'WFC',
                                       'Stock_max': '100988800.0',
                                       'YMD': '20161110'
                                   },
                                   {
                                       'Stock': 'WFC',
                                       'Stock_max': '100988800.0',
                                       'YMD': '20161110'
                                   },
                                   {
                                       'Stock': 'ORCL',
                                       'Stock_max': '86679100.0',
                                       'YMD': '20140919'
                                   },
                                   {
                                       'Stock': 'ORCL',
                                       'Stock_max': '86679100.0',
                                       'YMD': '20140919'
                                   },
                                   {
                                       'Stock': 'ORCL',
                                       'Stock_max': '86679100.0',
                                       'YMD': '20140919'
                                   },
                                   {
                                       'Stock': 'ORCL',
                                       'Stock_max': '86679100.0',
                                       'YMD': '20140919'
                                   },
                                   {
                                       'Stock': 'ORCL',
                                       'Stock_max': '86679100.0',
                                       'YMD': '20140919'
                                   },
                                   {
                                       'Stock': 'ORCL',
                                       'Stock_max': '86679100.0',
                                       'YMD': '20140919'
                                   }]
            },
            {
                'name':
                    'Customer2BatchAcp',
                'path':
                    "BUILD_DIR/src/data/qa/customer2/batch-acp.tar.gz",
                'Enabled':
                    True,
                'dstTable':
                    "acpchanges-per-sysid",
                'retinaParams': {
                    'pathToCustomer2Datasets':
                        'nfs:///freenas/datasets/customer2-small'
                },
                'expectedAnswer': [{
                    'sysid': '20110111786',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110115799',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110113203',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20111115221',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110112033',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110111561',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110116873',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110114201',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110113922',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110116963',
                    'numAcpChanges': '1'
                }, {
                    'sysid': '20110113669',
                    'numAcpChanges': '1'
                }]
            },
            {
                'name': 'LGLrq',
                'path': "BUILD_DIR/src/data/qa/LgDfg.tar.gz",
                'Enabled': True,
                'dstTable': "EndResult-LG",
                'retinaParams': {
                    'LgLrqSessionDataset':
                        '/netstore/datasets/lg_generated/sessionsSmall/'
                },
                'resultNumRowGMT': 942,
                'resultNumRowPDT': 1883
            }
        ]
    },
}
