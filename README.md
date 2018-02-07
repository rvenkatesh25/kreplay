# kreplay
Streaming PG query replay tool using Kafka

## Overview
Usually tools that replay Postgres query logs on another instance operate on a set of static log
files (e.g. [pgreplay](https://github.com/laurenz/pgreplay)). This implementation 
works in a streaming mode instead, which can be beneficial in cases where one needs to test a new
 PG version, or a new PG plugin/extension for an extended period of time. 
 
### Data Flow
1. Postgres is configured to log all queries to a file in csv format
2. A fluentd instance is configured to tail this csv file, parse the contents and forward the log
 lines to Kafka. Queries for each database are forwarded to its own topic.
3. One instance of kreplay is run per database, which tails the topic for the database and 
replays queries on the target PG instance

### Features
1. Original order in the csv log file is preserved during replay
2. Each session is replayed in a separate connection on the target instance
3. Transaction semantics on the origin sessions are therefore the same on the target

### Limitations
1. Limited throughput: the per-db Kafka topic has only one partition, and each instance of 
kreplay runs in a single thread to preserve ordering. 

## Setup
The following steps illustrate how to run a local instance of kreplay (on mac). 
Note: Currently the steps are for running all services directly on the laptop and not within any 
docker containers.

### Configure Postgres logging
Install postgres:
```
$ brew install postgresql@9.6
```

Set the following config values in /usr/local/var/postgres/postgresql.conf:
```
log_destination = 'csvlog'
logging_collector = on
log_directory = '/var/log/postgresql'
log_filename = 'postgresql-%I.log'
log_truncate_on_rotation = on
log_rotation_age = 1h
log_min_messages = notice
log_min_error_statement = notice
log_checkpoints = on
log_connections = on
log_disconnections = on
log_line_prefix = '%t'
log_lock_waits = on
log_statement = 'ddl'
log_replication_commands = on
log_temp_files = 0
log_timezone = 'UTC'
```
See [postgresql.conf.sample](postgresql.conf.sample) for a sample configuration file

Restart postgres:
```
$ brew services restart postgresql
```


### Configure Kafka instance
The simplest way is to run using [confluent tools](https://www.confluent.io/download/)

```
$ tar -xvzf confluent-oss-4.0.0-2.11.tar.gz
$ export PATH=$PATH:${pwd)/confluent-4.0.0/bin
$ confluent start zookeeper
$ confluent start kafka
$ confluent start kafka-rest
$ confluent status

```
Kafka-rest is available on [localhost:8082](http://localhost:8082). See [API Reference](https://docs.confluent.io/current/kafka-rest/docs/api.html).

### Configure Fluentd forwarding
Install fluentd and kafka forwarding plugin:
```
$ rbenv local 2.3.3
$ gem install fluentd -v "~> 0.12.0" --no-ri --no-rdoc
$ fluent-gem install fluent-plugin-kafka
```

See [fluentd.conf.example](fluent.conf.example) for fluentd forwarding configuration.

Start fluentd:
```
$ fluentd -c fluent.conf
```

## Kreplay Options
```
$ python kreplay.py -h
usage: kreplay.py [-h] [-t TOPIC] [-b BROKERS] [-d DB_NAME] [-u DB_USER]
                  [-p DB_PASSWORD] [-H DB_HOST] [-P DB_PORT] [-s SKIP_SELECTS]
                  [-m TIMEOUT_MS] [-a AFTER] [-i IGNORE_ERROR_SECONDS]

Replay postgres query streams from Kafka

optional arguments:
  -h, --help            show this help message and exit
  -t TOPIC, --topic TOPIC
                        Kafka topic to consume from
  -b BROKERS, --brokers BROKERS
                        Broker host:port, can be repeated
  -d DB_NAME, --db-name DB_NAME
                        Name of the database to replay queries to
  -u DB_USER, --db-user DB_USER
                        Username to connect to db
  -p DB_PASSWORD, --db-password DB_PASSWORD
                        Password to connect to db
  -H DB_HOST, --db-host DB_HOST
                        DB hostname to connect to
  -P DB_PORT, --db-port DB_PORT
                        DB port to connect to
  -s SKIP_SELECTS, --skip-selects SKIP_SELECTS
                        Do not replay select queries
  -m TIMEOUT_MS, --timeout-ms TIMEOUT_MS
                        Close lingering replay connection after this many
                        milliseconds
  -a AFTER, --after AFTER
                        Unix timestamp past which the queries will be
                        replayed. Setting to 0 disables this option (replay
                        all)
  -i IGNORE_ERROR_SECONDS, --ignore-error-seconds IGNORE_ERROR_SECONDS
                        Tolerate integrity errors for this many seconds. Can
                        be useful when the exact log line after a snapshot is
                        not known, so for a few seconds duplicate key sort of
                        errors should be ignored.Setting to -1 tolerates
                        errors forever. Setting to 0 fails on any error
```

Sample arguments to run with:
```
$ python kreplay.py -t pg_raw_thumbtack -b localhost:9092 -d thumbtack -u venky -H localhost -P 5434
```