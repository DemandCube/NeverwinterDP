package com.neverwinterdp.scribengin.dataflow.sample;

import com.beust.jcommander.Parameter;

/**
 * Simple class to house our configuration options for JCommander 
 */
public class WebEventRouterConfig {
  @Parameter(names = {"--help", "-h"}, help = true, description = "Output this help message")
  boolean help;

  @Parameter(names = "--local-app-home", required=true, description="The example dataflow local location")
  String localAppHome ;
  
  @Parameter(names = "--dfs-app-home", description="DFS location to upload the example dataflow")
  String dfsAppHome = "/applications/dataflow/sample";
  
  @Parameter(names = "--zk-connect", description="[host]:[port] of Zookeeper server")
  String zkConnect = "zookeeper-1:2181";
  
  @Parameter(names = "--hadoop-master-connect", description="Hostname of HadoopMaster")
  String hadoopMasterConnect = "hadoop-master";

  @Parameter(names = "--generator-num-of-web-events", description = "")
  int generatorNumOfWebEvents = 1000;
  
  @Parameter(names = "--dataflow-id", description = "Unique ID for the dataflow")
  String dataflowId  = "web-event-dataflow";
  
  @Parameter(names = "--dataflow-input-topic", description = "")
  String dataflowInputTopic  = "webevent";
  
  @Parameter(names = "--dataflow-input-topic-replication", description = "")
  int dataflowInputTopicReplication  = 2;
  
  @Parameter(names = "--dataflow-input-topic-partition", description = "")
  int dataflowInputTopicPartition  = 5;
  
  @Parameter(names = "--dataflow-default-replication", description = "Dataflow default replication")
  int dataflowDefaultReplication = 1;
  
  @Parameter(names = "--dataflow-default-parallelism", description = "The dataflow default parallelism")
  int dataflowDefaultParallelism = 5;
  
  @Parameter(names = "--dataflow-num-of-worker", description = "Number of workers to request")
  int dataflowNumOfWorker = 2;
  
  @Parameter(names = "--dataflow-num-of-executor-per-worker", description = "Number of Executors per worker to request")
  int dataflowNumOfExecutorPerWorker = 5;
  
  @Parameter(names = "--dataflow-tracking-window-size", description = "") 
  int dataflowTrackingWindowSize     = 1000;
  
  @Parameter(names = "--dataflow-slidding-window-size", description = "") 
  int dataflowSlidingWindowSize      = 15;

  @Parameter(names = "--dataflow-output-kafka-topic", description = "")
  String dataflowOutputKafkaTopic  = "output.archive";
  
  @Parameter(names = "--dataflow-output-hdfs-location", description = "")
  String dataflowOutputHdfsLocation  = "build/working/storage/hdfs";
  
  @Parameter(names = "--dataflow-output-es-addresses", description = "")
  String dataflowOutputESAddresses  = "elasticsearch-1:9300";
  
  @Parameter(names = "--dataflow-output-es-index", description = "")
  String dataflowOutputESIndex  = "webevent";
}