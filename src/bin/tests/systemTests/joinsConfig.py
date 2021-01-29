testCaseConfig = {
    'filter test': {
        'Enabled': False,
        'Description': 'test filter',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '100MB',
    },
    'Basic Test': {
        'Enabled': False,
        'Description': 'Basic test to illustrate how to add a new test case.',
        'ExpectedRunTime': 601,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '2GB',
    },
    'flight demo test': {
        'Enabled': False,
        'Description': 'test flight demo',
        'ExpectedRunTime': 60,    # In seconds for 1 run
        'NumRuns': 1    # Number of times a given user will invoke main()
    },
    'Multi join test': {
        'Enabled': True,
        'Description': 'test multi join',
        'ExpectedRunTime': 60,    # In seconds for 1 run
        'NumRuns': 1    # Number of times a given user will invoke main()
    },
    'Customer5 Test': {
        'Enabled':
            False,
        'Description':
            'Converted most of the Customer5 POC into a list of cli commands',
        'ExpectedRunTime':
            100000,    # In seconds for 1 run
        'NumRuns':
            1    # Number of times a given user will invoke main()
    },
    'XcalarSim': {
        'Enabled': False,
        'Description': 'Use Xcalar Sim to run random queries',
        'ExpectedRunTime': 100,    # In seconds for 1 run
        'NumRuns': 10    # Number of times a given user will invoke main()
    },
    'Simple Join Test': {
        'Enabled': True,
        'Description': 'test the different types of joins',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
        'datasetSize': '2GB',
    },
    'LG LRQ test': {
        'Enabled': False,
        'Description': '',
        'ExpectedRunTime': 600,    # In seconds for 1 run
        'NumRuns': 1,    # Number of times a given user will invoke main()
    },
}
