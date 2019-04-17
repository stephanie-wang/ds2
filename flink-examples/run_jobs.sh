#!/bin/bash


# Query 1 2, max throughput=600k.
for QUERY in 0 1 2;
do
    for THROUGHPUT in 150000 300000 450000 600000
    do
        bash run_job.sh $QUERY $THROUGHPUT
    done
done

# Query 3 stateful, max throughput=50k.
# Query 5, max throughput = 50k.
# Query 8, max throughput=40k.


for QUERY in 3Stateful 5 8;
do
    for THROUGHPUT in 10000 20000 30000 40000 50000
    do
        bash run_job.sh $QUERY $THROUGHPUT
    done
done
