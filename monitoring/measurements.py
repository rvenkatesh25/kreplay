class KafkaRecordsReceivedMeasurement(object):
    def __init__(self, topic, count):
        self.topic = topic
        self.records_read = count


class KafkaErrorsMeasurement(object):
    def __init__(self, topic, error_type):
        self.topic = topic
        self.error_type = error_type
        self.count = 1


class KafkaConsumerLatencyMeasurement(object):
    def __init__(self, topic, latency):
        self.topic = topic
        self.latency = latency


class PostgresReplayQueryMeasurement(object):
    def __init__(self, database):
        self.database = database


class PostgresErrorsMeasurement(object):
    def __init__(self, database, error_type):
        self.database = database
        self.error_type = error_type
        self.count = 1


class PostgresLatencyMeasurement(object):
    def __init__(self, database, operation, latency):
        self.database = database
        self.operation = operation
        self.latency = latency
