#!/bin/bash

hostname | grep ecehadoop
if [ $? -eq 1 ]; then
    echo "This script must be run on ecehadoop :("
    exit -1
fi

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/opt/hadoop-latest/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm Task2.jar
rm Task2*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task2.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task2.jar Task2*.class

echo --- Running
INPUT=/tmp/manualtestcase.txt
OUTPUT=/user/${USER}/a2_starter_code_output_hadoop/

$HADOOP_HOME/bin/hdfs dfs -rm -R $OUTPUT
#$HADOOP_HOME/bin/hdfs dfs -copyFromLocal sample_input/smalldata.txt /user/${USER}/
time $HADOOP_HOME/bin/yarn jar Task2.jar Task2 -D mapreduce.map.java.opts=-Xmx4g $INPUT $OUTPUT

$HADOOP_HOME/bin/hdfs dfs -ls $OUTPUT
$HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT/*
