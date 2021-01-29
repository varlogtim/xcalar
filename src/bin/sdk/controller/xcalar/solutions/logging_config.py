LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'simpleFormatter': {
            'format':
                '%(asctime)s.%(msecs)03d - Pid %(process)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
            'datefmt':
                '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'simpleFormatter',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',    # Default is stderr
        },
        'consoleHandler': {
            'level': 'DEBUG',
            'formatter': 'simpleFormatter',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',    # Default is stderr
        }
    },
    'loggers': {
        '': {    # root logger
            'handlers': ['consoleHandler'],
            'level': 'DEBUG',
            'propagate': False
        },
        'urllib3.connectionpool': {
            'handlers': ['consoleHandler'],
            'level': 'INFO',
            'propagate': False
        },
        'xcalar': {
            'handlers': ['consoleHandler'],
            'level': 'DEBUG',
            'propagate': False
        },
        'transitions.core': {
            'handlers': ['consoleHandler'],
            'level': 'DEBUG',
            'propagate': False
        },
        'xcalar.solutions.state_persister': {
            'handlers': ['consoleHandler'],
            'level': 'INFO',
            'propagate': False
        },
        '__main__': {    # if __name__ == '__main__'
            'handlers': ['consoleHandler'],
            'level': 'DEBUG',
            'propagate': False
        },
    }
}
