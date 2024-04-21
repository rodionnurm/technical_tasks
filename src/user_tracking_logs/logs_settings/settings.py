# base logger settings
LOGGING_CONFIG_BASE = {
    'version': 1,
    'disable_existing_loggers': False,

    'formatters': {
        'info': {
            'format': '%(asctime)s-%(levelname)s-%(name)s::%(module)s|%(lineno)s:: %(message)s'
        },
        'error': {
            'format': '%(asctime)s-%(levelname)s-%(name)s-%(process)d::%(module)s|%(lineno)s:: %(message)s'
        }
    },

    'handlers': {
        'debug_console_handler': {
            'level': 'DEBUG',
            'formatter': 'info',
            'class': 'logging.StreamHandler',
        }
    },

    'loggers': {
        '': {
            'level': 'NOTSET',
            'handlers': ['debug_console_handler']
        }
    }
}
