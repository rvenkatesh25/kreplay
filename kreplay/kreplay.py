import logging
import signal
import sys
import time

from dateutil.parser import parse
from kafka_receiver import KafkaReceiver
from pg_connector import PGConnector


class _PartitionProcessor:
    """Replay messages from a single partition of source Kafka topic

    Arguments:
        metrics (KreplayMonitoringClient): influxdb metrics object
        pg_connector (PGConnector): target postgres client object
        skip_selects (boolean): should select queries be skipped during replay
        session_timeout_ms (int): milliseconds after which a connection to the target
            postgres host should be closed, if we don't see a disconnect message
            from the stream in the meantime
        after (int): timestamp before which the incoming messages will not be
            considered for replay
    """
    def __init__(
            self,
            metrics,
            pg_connector,
            skip_selects,
            session_timeout_ms,
            after):
        self.metrics = metrics
        self.replay_commands = ['INSERT', 'DELETE', 'UPDATE', 'BREAK', 'COMMIT', 'ROLLBACK']
        self.pg_connector = pg_connector
        self.skip_selects = skip_selects
        self.session_timeout_ms = session_timeout_ms
        self.after = after

        self.session_timestamps = {}  # hash: session_id => timestamp of first statement
        self.last_seen_offset = None
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def is_valid_pg_message(pg_msg):
        """Check if all expected attributes exist in the message
        """

        if isinstance(pg_msg, dict) and \
                'command_tag' in pg_msg and \
                'statement' in pg_msg and \
                'session_id' in pg_msg:
            return True
        return False

    def _replay(self, session_id, statement):
        """Replay statement on the connection specific to given session id

        Arguments:
            session_id (str): session id to identify the connection
            statement (str): actual statement to be replayed
        """
        if session_id not in self.session_timestamps:
            self.session_timestamps[session_id] = PGConnector.now()
        return self.pg_connector.replay(session_id, statement)

    def _close_connection(self, session_id):
        """Close connection and remove session_id from dictionary

        Arguments:
            session_id (str): session identifier for the connection to be closed
        """
        if session_id in self.session_timestamps:
            if not self.pg_connector.close_connection(session_id):
                return False
            self.session_timestamps.pop(session_id)
        return True

    def _should_replay(self, command_tag, log_time):
        """Checks if log line is ready for replay based on config params

        Arguments:
            command_tag (str): command field in the pg log
            log_time (str): string representation of log time
        """
        try:
            parsed_log_time = int(parse(log_time).strftime("%s"))
        except ValueError:
            # always pass through un-parse-able date time
            parsed_log_time = sys.maxint

        # enabled replay of logs after the timestamp self.after 
        # replay the DML commands and select command if skip selects is not true
        return (parsed_log_time >= self.after) and (
                    (command_tag in ['SELECT', 'BIND', 'PARSE'] and not self.skip_selects) or
                    (command_tag in self.replay_commands)
                )

    def _should_end_session(self, command_tag, statement):
        """Session can be ended if we see a disconnect message

        Arguments:
            command_tag (str): command field in the pg log
            statement (str): actual pg statement
        """
        return command_tag == 'IDLE' and 'disconnection' in statement

    def prune_overdue_sessions(self):
        """Sessions lingering longer than timeout are force closed and purged from the
        session_timestamps dictionary. This enables the replay program to keep
        moving the consumer cursor forward
        """
        for session_id in self.session_timestamps.keys():
            if PGConnector.now() - self.session_timestamps[session_id] \
                    > self.session_timeout_ms:
                self.logger.warn('Closing overdue conn for session {} [timeout of {}ms]'
                                 .format(session_id, self.session_timeout_ms))
                self._close_connection(session_id)

    def process(self, record):
        """Process a given kafka record

        Arguments:
            record (KafkaRecord): a single message read from kafka
        """
        ret = True
        pg_msg = record.message
        if not _PartitionProcessor.is_valid_pg_message(pg_msg):
            self.logger.warn('Invalid message received from Postgres: {}'.format(pg_msg))
            return ret

        command_tag = pg_msg['command_tag'].upper()
        session_id = pg_msg['session_id']

        if self._should_replay(command_tag, pg_msg['log_time']):
            ret = self._replay(session_id, pg_msg['statement'])
        elif self._should_end_session(command_tag, pg_msg['statement']):
            ret = self._close_connection(session_id)

        self.last_seen_offset = record.offset
        return ret

    def can_commit(self):
        """It is safe to commit offset to kafka if there are no sessions in progress"""
        return len(self.session_timestamps) == 0

    def get_last_seen_offset(self):
        """Public api to fetch the last seen offset"""
        return self.last_seen_offset


class KReplay:
    """Replay postgres queries from a Kafka topic to a target PG cluster

    KReplay consumes query logs from a Kafka topic and replays it on the
    configured target postgres master. The Kafka topic needs to be necessarily
    configured with only one partition because only then it will be possible to
    replay queries in the same order as they were logged in the WAL on the source
    PG host.

    Arguments:
        metrics (KreplayMonitoringClient): influxdb metrics object
        topic (str): kafka topic to read queries from
        kafka_brokers (str array): list of bootstrap brokers
        db_name (str): name of target database
        db_user (str): user to replay query as
        db_pass (str): password for the configured user
        db_host (str): hostname for target database
        db_port (int): port for target database
        skip_selects (boolean): should select queries be skipped during replay
        session_timeout_ms (int): milliseconds after which a connection to the target
            postgres host should be closed, if we don't see a disconnect message
            from the stream in the meantime
        after (int): timestamp before which the incoming messages will not be
            considered for replay
        ignore_error_seconds (int): seconds for which referential integrity errors will
            be ignored. After this duration, the program will fail hard on errors
    """
    def __init__(
            self,
            metrics,
            topic='pg_raw_unmatched',
            kafka_brokers=None,
            db_name='postgres',
            db_user='postgres',
            db_pass='',
            db_host='localhost',
            db_port=5432,
            skip_selects=True,
            session_timeout_ms=60000,
            after=0,
            ignore_error_seconds=0):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.terminate = False

        self.metrics = metrics
        self.skip_selects = skip_selects
        self.session_timeout_ms = session_timeout_ms
        self.after = after
        self.kafka_receiver = KafkaReceiver(metrics, topic, kafka_brokers)
        self.pg_connector = PGConnector(metrics=metrics, db_name=db_name, db_user=db_user,
                                        db_pass=db_pass, db_host=db_host, db_port=db_port,
                                        ignore_error_seconds=ignore_error_seconds)

        self.processors = {}  # hash: partition => PartitionProcessor
        self.committed_offsets = {}  # hash: partition => committed offset
        self.logger = logging.getLogger(__name__)
        self.inactivity_sleep_seconds = 1

    def exit_gracefully(self, signum, frame):
        """Signal handler for SIGINT and SIGTERM from os"""
        self.terminate = True

    def run(self):
        """Main replay loop

        Note:
            A new _PartitionProcessor instance is created for each partition
            seen in the source topic.
        """
        err = False

        while not self.terminate and not err:
            # fetch messages from kafka
            try:
                records = self.kafka_receiver.get_next_records()
            except Exception:
                err = True
                return err

            # process messages, replay on session end
            for record in records:
                if record.partition not in self.processors:
                    self.logger.info('Adding processor for partition: {}'.format(record.partition))
                    self.processors[record.partition] = _PartitionProcessor(
                        self.metrics,
                        self.pg_connector,
                        self.skip_selects,
                        self.session_timeout_ms,
                        self.after
                    )
                    self.committed_offsets[record.partition] = 0
                if not self.processors[record.partition].process(record):
                    self.logger.error('Error encountered. Stopping')
                    err = True
                    break

            for partition, processor in self.processors.items():
                processor.prune_overdue_sessions()
                if processor.can_commit():
                    offset = processor.get_last_seen_offset()
                    if offset != self.committed_offsets[partition]:
                        self.logger.debug(
                            'Committing offset {} for partition {}'.format(offset, partition))
                        self.kafka_receiver.commit(partition, offset)
                        self.committed_offsets[partition] = offset

            if len(records) == 0:
                # nothing in the queue, take it easy
                time.sleep(self.inactivity_sleep_seconds)

        return err
