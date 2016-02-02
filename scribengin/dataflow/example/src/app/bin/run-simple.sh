#!/bin/bash

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

export HADOOP_USER_NAME="neverwinterdp"

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

JAVACMD=$JAVA_HOME/bin/java

$JAVACMD com.neverwinterdp.scribengin.dataflow.example.simple.SimpleDataflowExample  $@ 
