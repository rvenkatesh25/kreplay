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
### Configure Postgres logging
### Configure Kafka instance
### Configure Fluentd forwarding

## Kreplay Options
