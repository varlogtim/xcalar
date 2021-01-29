class CustomLogger():
    def __init__(self, logger):
        self.logger = logger
        self.prefix = ''

    def log(self, level, message):
        self.logger.log(level, f'{self.prefix} {message}')
