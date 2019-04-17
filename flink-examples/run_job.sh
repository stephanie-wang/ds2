#!/bin/bash

QUERY=$1
THROUGHPUT=$2

if [ $# != 2 ];
then
    echo "Usage: ./run_job.sh <query number> <src throughput>"
    exit
fi

DURATION=60
FLINK=~/flink-1.7.2
NUM_EVENTS=$(( $THROUGHPUT * $DURATION ))
FILENAME="latency-"$QUERY"-query-"$THROUGHPUT"-throughput-1-workers.txt"
if ls $FILENAME 1> /dev/null 2>&1
then
    echo "Output $FILENAME already found, skipping..."
    sleep 1
    exit
fi
echo "Logging to $FILENAME"

$FLINK/bin/stop-cluster.sh
rm $FLINK/log/*
$FLINK/bin/start-cluster.sh
sleep 1


$FLINK/bin/flink run -c ch.ethz.systems.strymon.ds2.flink.nexmark.queries.Query$QUERY target/flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar --srcRate $THROUGHPUT --numEvents $NUM_EVENTS

bash collect_latencies.sh $QUERY $THROUGHPUT $FILENAME
