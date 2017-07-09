#!/usr/bin/env bash

docker run -it --name=spark -p 7777:7777 -p 8888:8888 -p 9000:9000 -p 8088:8088 -p 8031:8031 -p 8032:8032 -p 8033:8033 -p 50070:50070 -p 50075:50075 -p 50030:50030 -v $(pwd)/data:/data cithub/spark-yarn
# following inside giraph container
hdfs namenode -format
./home/hadoop/hadoop/start.sh
hdfs dfs -mkdir /tmp
cd /data/tmp
hdfs dfs -put tiny-graph.txt /tmp/tiny-graph.txt
hdfs dfs -ls /tmp

export SPARK_HOME=/home/hadoop/spark/spark
export YARN_CONF_DIR=/home/hadoop/hadoop/etc/hadoop
export PYSPARK_DRIVER_PYTHON=ipython3
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8888"
export PYSPARK_PYTHON=python3

cd $SPARK_HOME
./bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --queue default \
    examples/jars/spark-examples_2.11-2.1.1.jar \
    2

jupyter kernelspec list
jupyter notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root

docker exec spark hdfs dfs -mkdir /data
docker exec spark hdfs dfs -put data/yago/yagoFacts.tsv /data/yagoFacts.tsv