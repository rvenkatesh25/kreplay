"""
This module allows programs to record metrics to Thumbtack's monitoring
system(s).

The core of this class are the Backends and the Client. The Client will
convert and forward measurement objects to the client's Backend, which
performs the actual recording of the data.

To record measurements, a measurement object must be passed to a Client's
measure method.  Any string or boolean properties will be converted to tags,
and any numeric values will be converted to fields.

    from tophat.lib.monitoring import Influxdbd

    metrics = Influxdbd('staging')

    class SampleMeasurement(object):
        def __init__(self, atag, bfield):
            self.atag = atag
            self.bfield = bfield

    measurement = SampleMeasurement('classifier', 55.23)
    metrics.measure(measurement)
"""
import logging
import math
import multiprocessing
import os
import Queue
import socket
import threading
import time
import urllib

BUFFER_SIZE = 500
FLUSH_TIME = 4
RETRY_COUNT = 2
TIMEOUT = 4


def time_to_nanoseconds(now):
    """Convert time.time() to a nanosecond string"""
    right, left = math.modf(now)
    return '{}{}'.format(
        int(left),
        str(int(right * 1000000000)).zfill(9)
    )


def convert_attribute(attr):
    """Convert a variable to a field and/or tag"""
    if attr is None or callable(attr):
        return None, None
    if isinstance(attr, bool):
        return u'true' if attr else u'false', None
    if isinstance(attr, int):
        return None, u'{}i'.format(attr)
    if isinstance(attr, float):
        right, left = math.modf(attr)
        return None, u'{}.{}'.format(int(left), str(right)[2:])
    if isinstance(attr, str):
        return attr, None
    raise TypeError('Unmeasurable type: {}'.format(type(attr)))


class InfluxDBBackendThread(threading.Thread):
    """A thread that forwards data to InfluxDB"""
    def __init__(self, queue, endpoint):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stopper = threading.Event()
        self.queue = queue
        self.endpoint = endpoint
        self._logger = logging.getLogger(__name__)

    def _write(self, data, retries=RETRY_COUNT):
        if len(data) == 0:
            return True
        if retries < 0:
            return False
        self._logger.info('Trying to log to server, retries: %d', retries)
        try:
            urllib.urlopen(self.endpoint, data, TIMEOUT)
            return True
        except socket.timeout:
            self._logger.error('Socket timed out.')
        except OSError as err:
            self._logger.warning('Failed to write metric data: %s - %s', err, data)
        self._write(data, retries - 1)

    def run(self):
        self._logger.info('Starting background thread...')
        points = []
        last_write = time.time()
        while True:
            try:
                while len(points) < BUFFER_SIZE:
                    wait = max(0, time.time() - last_write)
                    points.append(self.queue.get(True, wait))
            except Queue.Empty:
                pass
            now = time.time()
            if len(points) >= BUFFER_SIZE or now >= last_write + FLUSH_TIME:
                binary_data = ''.join(points).encode('utf-8')
                success = self._write(binary_data)
                if not success:
                    self._logger.warning(
                        'Dropped %d metrics due to write failures',
                        len(points)
                    )
                last_write = now
                points = []
                if self.stopper.is_set():
                    return

    def stop(self):
        """Stop running after the queue is flushed"""
        self.stopper.set()


class InfluxDBBackend(object):
    """
    A backend for writing data to InfluxDB

    :param str environment: The alfred environment 
    :param str database: The name of the database to write to
    :param str username: The user to write with
    :param str password: The password of the provided user
    """
    def __init__(self, environment, database, username, password):
        self._logger = logging.getLogger(__name__)
        endpoint = 'http://{}-influxdb:8086/write?db={}&u={}&p={}&precision=n'.format(
            environment,
            urllib.quote(database),
            urllib.quote(username),
            urllib.quote(password)
        )
        self._logger.info('Endpoint info: %s', endpoint)
        self._queue = multiprocessing.Queue(BUFFER_SIZE)
        self._thread = InfluxDBBackendThread(self._queue, endpoint)
        self._thread.start()

    def write(self, point):
        """Buffer a point for asynchronous flushing"""
        try:
            self._queue.put_nowait(point)
        except Queue.Full:
            logging.warning('Failed to record metric data point: queue full')

    def close(self):
        """Stop the flushing thread and wait for all points to be flushed"""
        self._thread.stop()
        self._thread.join()


class ListBackend(object):
    """
    Client backend for storing data points in a list

    This backend will store data in an in-memory list. 'contains()' can be used
    to check if a specific point has been recorded yet.
    """
    def __init__(self):
        self.data = []
        self._logger = logging.getLogger(__name__)

    def write(self, point):
        """Store a point in the local list"""
        self.data.append(point)
        self._logger.debug('Monitoring datapoint {}'.format(point))

    def contains(self, point):
        """Check if a point has been recorded"""
        return point in self.data

    def close(self):
        """Do nothing (for compatibility with other backends)"""
        pass


class Client(object):
    """
    Class for recording measurements

    This class is built from a backend and global tags. When data points are
    measured, the global tags will be added to the final result and the final
    data point will be sent to the backend.
    """
    def __init__(self, backend, tags=None, now=time.time):
        self.backend = backend
        self.tags = {} if tags is None else tags
        self.now = now
        self._logger = logging.getLogger(__name__)

    def measure(self, obj):
        """
        Store the measurement data point described by the object

        measure() can only measure objects with string, boolean, or numeric
        attributes, and will throw an error if it encounters more complex
        attributes (lists, dicts, objects...).

        :raises: TypeError
        """
        now = time_to_nanoseconds(self.now())
        measurement = obj.__class__.__name__
        if measurement.endswith('Measurement'):
            measurement = measurement[:-len('Measurement')]
        fields = {}
        tags = {}
        elements = (i for i in dir(obj) if not i.startswith('_'))
        for element in elements:
            tag, field = convert_attribute(getattr(obj, element))
            if tag is not None:
                tags[element] = tag
            if field is not None:
                fields[element] = field
        if len(fields) == 0:
            fields['count'] = 1
        for key, value in self.tags.items():
            tags[key] = value
        # inject environment tag
        if os.environ.get('ALFRED_ENVIRONMENT') is not None:
            tags['environment'] = os.environ.get('ALFRED_ENVIRONMENT')
        else:
            tags['environment'] = 'local'
        tag_set = ','.join(
            '{}={}'.format(k, tags[k]) for k in sorted(tags.keys())
        )
        field_set = ','.join(
            '{}={}'.format(k, fields[k]) for k in sorted(fields.keys())
        )
        point = '{},{} {} {}\n'.format(measurement, tag_set, field_set, now)
        self._logger.debug('Measuring %s', point)
        self.backend.write(point)

    def close(self):
        """Flush all points and close the client"""
        self.backend.close()
