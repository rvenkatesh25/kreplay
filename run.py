#! /usr/bin/env python
import argparse
import os
import sys
import SimpleHTTPServer
import SocketServer
from kreplay import KReplay
from threading import Thread


def health_check():
    configured_port = os.getenv('ALFRED_PORT')
    if configured_port is None:
        port = 17170  # default local port
    else:
        port = int(configured_port)
    # TODO: serve only /health
    httpd = SocketServer.TCPServer(("", port), SimpleHTTPServer.SimpleHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    # spawn a background thread for health checks
    hc_thread = Thread(target=health_check, args=())
    hc_thread.daemon = True
    hc_thread.start()

    parser = argparse.ArgumentParser(description='Replay postgres query streams from Kafka')
    parser.add_argument('-t', '--topic', default='pg_raw_unmatched',
                        help='Kafka topic to consume from')
    parser.add_argument('-b', '--brokers', default=[], action='append',
                        help='Broker host:port, can be repeated')
    parser.add_argument('-d', '--db-name', default='postgres',
                        help='Name of the database to replay queries to')
    parser.add_argument('-u', '--db-user', default='postgres',
                        help='Username to connect to db')
    parser.add_argument('-p', '--db-password', default='',
                        help='Password to connect to db')
    parser.add_argument('-H', '--db-host', default='localhost',
                        help='DB hostname to connect to')
    parser.add_argument('-P', '--db-port', default=5432,
                        help='DB port to connect to')
    parser.add_argument('-s', '--skip-selects', default=True, action='store',
                        help='Do not replay select queries')
    parser.add_argument('-m', '--timeout-ms', default=60000,
                        help='Close lingering replay connection after this many milliseconds')
    parser.add_argument('-a', '--replay-after', default=0,
                        help='Unix timestamp past which the queries will be replayed. '
                             'Setting to 0 disables this option (replay all)')
    parser.add_argument('-i', '--ignore-error-seconds', default=0,
                        help='Tolerate integrity errors for this many seconds. Can be useful '
                             'when the exact log line after a snapshot is not known, so for '
                             ' a few seconds duplicate key sort of errors should be ignored.'
                             'Setting to -1 tolerates errors forever. '
                             'Setting to 0 fails on any error')

    args = parser.parse_args()

    topic = os.getenv('topic') or args.topic

    brokers = []
    brokers_env = os.getenv('brokers')
    if brokers_env is not None:
        brokers = brokers_env.split(',')
    else:
        brokers = args.brokers

    db_name = os.getenv('dbName') or args.db_name
    db_user = os.getenv('dbUser') or args.db_user
    db_password = os.getenv('dbPassword') or args.db_password
    db_host = os.getenv('dbHost') or args.db_host
    db_port = int(os.getenv('dbPort')) if os.getenv('dbPort') is not None else args.db_port
    skip_selects = bool(os.getenv('skipSelects')) if os.getenv('skipSelects') is not None else \
        args.skip_selects
    timeout_ms = int(os.getenv('timeoutMS')) if os.getenv('timeoutMS') is not None else \
        args.timeout_ms
    replay_after = int(os.getenv('replayAfter')) if os.getenv('replayAfter') is not None else \
        args.replay_after
    ignore_error_seconds = int(os.getenv('ignoreErrorSeconds')) if os.getenv(
        'ignoreErrorSeconds') is not None else args.ignore_error_seconds

    app = KReplay(
        topic=topic,
        kafka_brokers=brokers,
        db_name=db_name,
        db_user=db_user,
        db_pass=db_password,
        db_host=db_host,
        db_port=db_port,
        skip_selects=skip_selects,
        session_timeout_ms=timeout_ms,
        after=replay_after,
        ignore_error_seconds=ignore_error_seconds,
    )
    error = app.run()
    if error:
        sys.exit(1)
    sys.exit(0)
