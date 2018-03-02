import logging
import os
import sys

from common.monitoring import Client, InfluxDBBackend, ListBackend


class KreplayMonitoringClient(Client):
    def __init__(self, app_name='kreplay'):
        logging.getLogger(__name__).info('Initializing Influxdb client')
        if os.environ.get('ALFRED_ENVIRONMENT') is not None:
            password = os.environ['VAULT_INFLUXDB_WRITE']
            try:
                backend = InfluxDBBackend(app_name, '{}_write'.format(app_name), password)
            except:
                logging.getLogger(__name__).info(sys.exc_info()[0])
                raise
        else:
            # no metrics client for local runs
            backend = ListBackend()

        Client.__init__(self, backend)

