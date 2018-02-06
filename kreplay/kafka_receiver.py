import collections
import msgpack

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from utils import Log

KafkaRecord = collections.namedtuple('KafkaRecord', ['partition', 'offset', 'message'])


class KafkaReceiver:
    def __init__(
            self,
            topic,
            kafka_brokers,
            max_records_per_get=100,
            get_messages_timeout_ms=100,
            max_retries=3):
        self.topic = topic
        self.max_records_per_get = max_records_per_get
        self.get_messages_timeout_ms = get_messages_timeout_ms

        consumer_group = 'kreplay_{}'.format(topic)
        Log.info('Starting Kafka consumer for topic: {}'.format(topic))
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_brokers,
            group_id=consumer_group,
            value_deserializer=msgpack.loads,
            enable_auto_commit=False,
            auto_offset_reset='latest',
        )
    
    def get_next_records(self):
        records = self.consumer.poll(
            max_records=self.max_records_per_get,
            timeout_ms=self.get_messages_timeout_ms
        )

        return_records = []
        for tp, msgs in records.items():
            for msg in msgs:
                if not KafkaReceiver.is_valid_kafka_message(msg):
                    Log.error("Invalid kafka message: {}".format(msg))
                    continue
                else:
                    return_records.append(KafkaRecord(tp.partition, msg.offset, msg.value))
        return return_records

    def commit(self, partition, offset):
        if offset is None:
            return
        self.consumer.commit({
            TopicPartition(self.topic, partition): OffsetAndMetadata(offset, None)
        })
        
    @staticmethod
    def is_valid_kafka_message(msg):
        if hasattr(msg, 'value') and hasattr(msg, 'offset'):
            return True
        return False


