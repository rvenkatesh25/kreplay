import logging
import psycopg2
import time

from datetime import datetime
from retrying import retry
from monitoring import PostgresReplayQueryMeasurement, PostgresErrorsMeasurement, \
    PostgresLatencyMeasurement


class PGConnector:
    """Postgres client to connect to target cluster and execute replay queries

    Arguments:
        Arguments:
        metrics (KreplayMonitoringClient): influxdb metrics object
        db_name (str): name of target database
        db_user (str): user to replay query as
        db_pass (str): password for the configured user
        db_host (str): hostname for target database
        db_port (int): port for target database
        ignore_error_seconds (int): seconds for which referential integrity errors will
            be ignored. After this duration, the program will fail hard on errors
    """
    def __init__(self, metrics, db_name, db_user, db_pass, db_host, db_port, ignore_error_seconds):
        self.metrics = metrics
        self.db_name = db_name
        self.connection_string = 'dbname={} user={} password={} host={} port={}'.format(
            db_name, db_user, db_pass, db_host, db_port
        )
        self.first_timestamp = 0
        self.ignore_error_seconds = ignore_error_seconds
        self.connections = {}  # hash: session_id => connection
        self.logger = logging.getLogger(__name__)

    def close_connection(self, session_id):
        """Close the connection identified by the session id

        Arguments:
            session_id (str): session id to identify the connection
        """
        if session_id in self.connections:
            self.logger.info('Closing connection for session {}'.format(session_id))
            try:
                self._close(session_id)
            except Exception as e:
                self.logger.error(
                    'Error closing connection for session {}\n{}'.format(session_id, e))
                self.metrics.measure(PostgresErrorsMeasurement(
                    self.db_name, 'ConnectionCloseError'))
                return False
        return True

    def replay(self, session_id, statement):
        """Replay the statement on a connection specific to this session

        Arguments:
            session_id (str): session id to identify the connection
            statement (str): pg statement to be replayed
        """
        if session_id not in self.connections or self.connections[session_id] is None:
            start = datetime.now()
            try:
                self._connect(session_id)
                if self.first_timestamp == 0:
                    self.first_timestamp = PGConnector.now()
            except Exception as e:
                self.logger.error(
                    'Error closing connection for session {}\n{}'.format(session_id, e))
                self.metrics.measure(PostgresErrorsMeasurement(self.db_name, 'ConnectionError'))
                return False
            finally:
                end = datetime.now()
                self.metrics.measure(PostgresLatencyMeasurement(
                    self.db_name, 'Connect', (end-start).total_seconds()))

        start = datetime.now()
        try:
            query = PGConnector.cleanup_statement(statement)
            self.logger.info('Will replay query: {} w/ session {}'.format(query, session_id))

            self._execute(session_id, query)
            self.metrics.measure(PostgresReplayQueryMeasurement(self.db_name))
        except Exception as e:
            if isinstance(e, psycopg2.IntegrityError) and self._can_ignore_error():
                self.logger.warn(
                    'SKIPPING: Statement violates constraint: {}\n{}'.format(statement, e))
                self.metrics.measure(PostgresErrorsMeasurement(self.db_name, 'SkipIntegrityError'))
            else:
                self.logger.error('Error executing statement: {}\n{}: {}'.format(
                    statement, type(e).__name__, e))
                self.metrics.measure(PostgresErrorsMeasurement(self.db_name, 'QueryError'))
                self._close(session_id)
                return False
        finally:
            end = datetime.now()
            self.metrics.measure(PostgresLatencyMeasurement(
                self.db_name, 'Query', (end-start).total_seconds()))
        return True

    def _can_ignore_error(self):
        """Ignore errors for specified number of seconds"""
        return PGConnector.now() - self.first_timestamp < self.ignore_error_seconds

    @staticmethod
    def now():
        """Helper to return current timestamp in millis"""
        return int(round(time.time() * 1000))

    @staticmethod
    def cleanup_statement(statement):
        """Do some csv parsing magic

        Arguments:
            statement (str): pg statement
        """
        # In PG CSV double quotes in field values are escaped with double quotes
        # (https://www.ietf.org/rfc/rfc4180.txt)
        # This impacts json values. E.g.
        #   {""device_fingerprint"":""a""}
        # Need to convert this to:
        #   {"device_fingerprint":"a"}
        if statement.strip() == '""':
            return statement
        return statement.replace('""', '"')

    @retry(stop_max_attempt_number=3, wait_fixed=500)
    def _connect(self, session_id):
        """Create a new connection for the given session id

        Arguments:
            session_id (str): session id to associate the connection with
        """
        self.connections[session_id] = psycopg2.connect(self.connection_string)
        self.connections[session_id].set_session(autocommit=True)
        return True

    @retry(stop_max_attempt_number=3, wait_fixed=500)
    def _close(self, session_id):
        """Close the connection identified by session id

        Arguments:
            session_id (str): session id to identify the connection
        """
        if self.connections[session_id] is not None:
            self.connections[session_id].close()
        return True

    @retry(stop_max_attempt_number=3, wait_fixed=500)
    def _execute(self, session_id, query):
        """Execute query on the connection identified by session id

        Arguments:
            session_id (str): session id to identify the connection
            query (str): actual query to be executed
        """
        if not query:
            # empty query is a noop success
            return True

        assert session_id in self.connections
        assert self.connections[session_id] is not None

        cur = self.connections[session_id].cursor()
        cur.execute(query)
