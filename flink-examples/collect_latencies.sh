#!/bin/bash

QUERY=$1
THROUGHPUT=$2
FILENAME=$3
FLINK=~/flink-1.7.2

if [ $# != 3 ];
then
    echo "Usage: ./collect_latencies.sh <query number> <src throughput>"
    exit
fi

grep nexmark.queries $FLINK/log/flink-*-taskexecutor-*.log | grep latency | awk -F% '{ print $3 }' 2>&1 | tee $FILENAME

count=0
total=0
for i in $( awk '{ print $0; }' $FILENAME );
do
    total=$(echo $total+$i | bc );
    ((count++))
done;
echo "mean latency:$(( $total / $count ))" | tee -a $FILENAME

echo "target throughput:$THROUGHPUT" | tee -a $FILENAME
for type in Bid Auction Person
do
    tput=`grep THROUGHPUT $FLINK/log/flink-*-taskexecutor-*.log | grep $type | awk -F"THROUGHPUT: " '{ print $2 }'`
    if [[ $tput -eq "" ]];
    then
        tput=`grep throughput $FLINK/log/flink-*-taskexecutor-*.log | grep $type | tail -n 1 | awk -F"throughput: " '{ print $2 }'`
    fi
    if [[ $tput -ne "" ]];
    then
        echo "$type actual throughput: $tput" | tee -a $FILENAME
    fi
done
