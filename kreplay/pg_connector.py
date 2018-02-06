import psycopg2

from retrying import retry
from utils import Log, now


class PGConnector:
    def __init__(self, db_name, db_user, db_pass, db_host, db_port, ignore_error_seconds):
        self.connection_string = 'dbname={} user={} password={} host={} port={}'.format(
            db_name, db_user, db_pass, db_host, db_port
        )
        self.first_timestamp = 0
        self.ignore_error_seconds = ignore_error_seconds
        self.connections = {}  # hash: session_id => connection
        pass

    def close_connection(self, session_id):
        if session_id in self.connections:
            Log.info('Closing connection for session {}'.format(session_id))
            try:
                self._close(session_id)
            except Exception as e:
                Log.error('Error creating connection for session {}\n{}'.format(session_id, e))
                return False
        return True

    def replay(self, session_id, statement):
        if session_id not in self.connections or self.connections[session_id] is None:
            try:
                self._connect(session_id)
                if self.first_timestamp == 0:
                    self.first_timestamp = now()
            except Exception as e:
                Log.error('Error closing connection for session {}\n{}'.format(session_id, e))
                return False

        try:
            query = PGConnector.cleanup_statement(statement)
            Log.info('Will replay query: {} w/ session {}'.format(query, session_id))
            self._execute(session_id, query)
        except Exception as e:
            if isinstance(e, psycopg2.IntegrityError) and self._can_ignore_error():
                Log.warn('SKIPPING: Statement violates constraint: {}\n{}'.format(statement, e))
            else:
                Log.error('Error executing statement: {}\n{}: {}'.format(
                    statement, type(e).__name__, e))
                self._close(session_id)
                return False
        return True

    def _can_ignore_error(self):
        return now() - self.first_timestamp < self.ignore_error_seconds

    @staticmethod
    def cleanup_statement(statement):
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
        self.connections[session_id] = psycopg2.connect(self.connection_string)
        self.connections[session_id].set_session(autocommit=True)
        return True

    @retry(stop_max_attempt_number=3, wait_fixed=500)
    def _close(self, session_id):
        if self.connections[session_id] is not None:
            self.connections[session_id].close()
        return True

    @retry(stop_max_attempt_number=3, wait_fixed=500)
    def _execute(self, session_id, query):
        if not query:
            # empty query is a noop success
            return True

        assert session_id in self.connections
        assert self.connections[session_id] is not None

        cur = self.connections[session_id].cursor()
        cur.execute(query)
