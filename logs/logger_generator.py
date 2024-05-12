class Loggen():
    def __init__(self):
        self.process = []

    def logs_error(self, logger, logs):
        self.process.append(logs)
        logger.error(logs)
        return logs

    def logs_info(self, logger, logs):
        logger.info(logs)
        return logs