import collections
import logging
import msgpack

from datetime import datetime
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from monitoring import KafkaRecordsReceivedMeasurement, KafkaErrorsMeasurement, \
    KafkaConsumerLatencyMeasurement

KafkaRecord = collections.namedtuple('KafkaRecord', ['partition', 'offset', 'message'])


class KafkaReceiver:
    def __init__(
            self,
            metrics,
            topic,
            kafka_brokers,
            max_records_per_get=100,
            get_messages_timeout_ms=100,
            max_retries=3):
        self.metrics = metrics
        self.topic = topic
        self.max_records_per_get = max_records_per_get
        self.get_messages_timeout_ms = get_messages_timeout_ms
        self.logger = logging.getLogger(__name__)

        consumer_group = 'kreplay_{}'.format(topic)
        self.logger.info('Starting Kafka consumer for topic: {}'.format(topic))
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_brokers,
                group_id=consumer_group,
                value_deserializer=msgpack.loads,
                enable_auto_commit=False,
                auto_offset_reset='latest',
            )
        except Exception as e:
            self.logger.error('Cannot start consumer. Error: {}'.format(e.message))
            self.metrics.measure(KafkaErrorsMeasurement(self.topic, 'ConnectionError'))
            raise e
    
    def get_next_records(self):
        # infinite retries on connection errors, with exponential backoff
        # between 50ms to 1s
        # get_messages_timeout_ms it not honored on connection errors
        start = datetime.now()
        records = self.consumer.poll(
            max_records=self.max_records_per_get,
            timeout_ms=self.get_messages_timeout_ms
        )
        end = datetime.now()
        self.metrics.measure(
            KafkaConsumerLatencyMeasurement(self.topic, (end-start).total_seconds()))

        return_records = []
        for tp, msgs in records.items():
            for msg in msgs:
                if not KafkaReceiver.is_valid_kafka_message(msg):
                    self.logger.error('Invalid kafka message: {}'.format(msg))
                    self.metrics.measure(KafkaErrorsMeasurement(self.topic, 'InvalidMessage'))
                    continue
                else:
                    return_records.append(KafkaRecord(tp.partition, msg.offset, msg.value))
        self.metrics.measure(KafkaRecordsReceivedMeasurement(self.topic, len(return_records)))
        return return_records

    def commit(self, partition, offset):
        if offset is None:
            return
        try:
            self.consumer.commit({
                TopicPartition(self.topic, partition): OffsetAndMetadata(offset, None)
            })
        except Exception as e:
            self.logger.error('Cannot commit offset {} for topic:partition {}:{}. Error: {}'
                              .format(offset, self.topic, partition, e.message))
            self.metrics.measure(KafkaErrorsMeasurement(self.topic, 'CommitError'))
            raise e
        
    @staticmethod
    def is_valid_kafka_message(msg):
        if hasattr(msg, 'value') and hasattr(msg, 'offset'):
            return True
        return False


