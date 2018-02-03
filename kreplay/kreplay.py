import argparse
import signal
import sys
import time

from kafka_receiver import KafkaReceiver
from pg_connector import PGConnector
from utils import Log


class _PartitionProcessor:
    def __init__(
            self,
            pg_connector,
            skip_selects,
            session_timeout_ms):
        self.replay_commands = ['INSERT', 'DELETE', 'UPDATE', 'BREAK', 'COMMIT', 'ROLLBACK']
        self.pg_connector = pg_connector
        self.skip_selects = skip_selects
        self.session_timeout_ms = session_timeout_ms

        self.session_timestamps = {}  # hash: session_id => timestamp of first statement
        self.last_seen_offset = None

    @staticmethod
    def is_valid_pg_message(pg_msg):
        if 'command_tag' in pg_msg:
            return True
        return False

    @staticmethod
    def now():
        return int(round(time.time() * 1000))

    def _replay(self, session_id, statement):
        if session_id not in self.session_timestamps:
            self.session_timestamps[session_id] = _PartitionProcessor.now()
        return self.pg_connector.replay(session_id, statement)

    def _close_connection(self, session_id):
        if session_id in self.session_timestamps:
            if not self.pg_connector.close_connection(session_id):
                return False
            self.session_timestamps.pop(session_id)
        return True

    def _should_replay(self, command_tag):
        # replay the DML commands and select command if skip selects is not true
        return (command_tag == 'SELECT' and not self.skip_selects) or \
               (command_tag in self.replay_commands)

    def _should_end_session(self, command_tag, statement):
        return command_tag == 'IDLE' and 'disconnection' in statement

    def prune_overdue_sessions(self):
        for session_id in self.session_timestamps.keys():
            if _PartitionProcessor.now() - self.session_timestamps[session_id] \
                    > self.session_timeout_ms:
                Log.warn('Closing overdue conn for session {} [timeout of {}ms]'
                         .format(session_id, self.session_timeout_ms))
                self._close_connection(session_id)

    def process(self, record):
        ret = True
        pg_msg = record.message
        if not _PartitionProcessor.is_valid_pg_message(pg_msg):
            Log.warn('Invalid message received from Postgres: {}'.format(pg_msg))
            return

        command_tag = pg_msg['command_tag'].upper()
        session_id = pg_msg['session_id']

        if self._should_replay(command_tag):
            ret = self._replay(session_id, pg_msg['statement'])
        elif self._should_end_session(command_tag, pg_msg['statement']):
            ret = self._close_connection(session_id)

        self.last_seen_offset = record.offset
        return ret

    def can_commit(self):
        return len(self.session_timestamps) == 0

    def get_last_seen_offset(self):
        return self.last_seen_offset


class KReplay:
    def __init__(
            self,
            topic='pg_raw_unmatched',
            kafka_brokers=None,
            db_name='postgres',
            db_user='postgres',
            db_pass='',
            db_host='localhost',
            db_port=5432,
            skip_selects=True,
            session_timeout_ms=60000):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.terminate = False

        self.skip_selects = skip_selects
        self.session_timeout_ms = session_timeout_ms
        self.kafka_receiver = KafkaReceiver(topic, kafka_brokers)
        self.pg_connector = PGConnector(db_name=db_name, db_user=db_user, db_pass=db_pass,
                                        db_host=db_host, db_port=db_port)

        self.processors = {}  # hash: partition => PartitionProcessor
        self.committed_offsets = {}  # hash: partition => committed offset

    def exit_gracefully(self, signum, frame):
        self.terminate = True

    def run(self):
        err = False

        while not self.terminate and not err:
            # fetch messages from kafka
            records = self.kafka_receiver.get_next_records()

            # process messages, replay on session end
            for record in records:
                if record.partition not in self.processors:
                    Log.info('Adding processor for partition: {}'.format(record.partition))
                    self.processors[record.partition] = _PartitionProcessor(
                        self.pg_connector,
                        self.skip_selects,
                        self.session_timeout_ms
                    )
                    self.committed_offsets[record.partition] = 0
                if not self.processors[record.partition].process(record):
                    Log.error('Error encountered. Stopping')
                    err = True
                    break

            for partition, processor in self.processors.items():
                processor.prune_overdue_sessions()
                if processor.can_commit():
                    offset = processor.get_last_seen_offset()
                    if offset != self.committed_offsets[partition]:
                        Log.debug('Committing offset {} for partition {}'.format(offset, partition))
                        self.kafka_receiver.commit(partition, offset)
                        self.committed_offsets[partition] = offset

        return err

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replay postgres query streams from Kafka')
    parser.add_argument('-t', '--topic', default='pg_raw_unmatched')
    parser.add_argument('-b', '--brokers', default=[], action='append')
    parser.add_argument('-d', '--db-name', default='postgres')
    parser.add_argument('-u', '--db-user', default='postgres')
    parser.add_argument('-p', '--db-password', default='')
    parser.add_argument('-H', '--db-host', default='localhost')
    parser.add_argument('-P', '--db-port', default=5432)

    args = parser.parse_args()

    app = KReplay(topic=args.topic, kafka_brokers=args.brokers,
                  db_name=args.db_name, db_user=args.db_user, db_pass=args.db_password,
                  db_host=args.db_host, db_port=args.db_port)
    error = app.run()
    if error:
        sys.exit(1)

    Log.info('Shutting down kreplay')
    sys.exit(0)