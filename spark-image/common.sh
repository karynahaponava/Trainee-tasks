#!/bin/bash

export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-spark-master}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8080}
export SPARK_MASTER_LOG=/opt/spark/logs

mkdir -p $SPARK_MASTER_LOG