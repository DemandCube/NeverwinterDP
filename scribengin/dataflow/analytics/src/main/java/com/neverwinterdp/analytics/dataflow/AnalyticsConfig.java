package com.neverwinterdp.analytics.dataflow;

import com.beust.jcommander.Parameter;

public class AnalyticsConfig {
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

  @Parameter(names = "--generator-odyssey-input-topic", description="")
  String generatorOdysseyInputTopic = "odyssey.input";
  
  @Parameter(names = "--generator--odyssey-num-of-events", description="")
  int generatorOdysseyNumOfEvents = 100000;
  
  @Parameter(names = "--generator-web-input-topic", description="")
  String generatorWebInputTopic = "web.input";
  
  @Parameter(names = "--generator--web-num-of-events", description="")
  int generatorWebNumOfEvents = 100000;
  
  @Parameter(names = "--dataflow-id", description="")
  String dataflowId         = "analytics";
  
  @Parameter(names = "--dataflow-default-parallelism", description="")
  int    dataflowDefaultParallelism = 5;
  
  @Parameter(names = "--dataflow-default-replication", description="")
  int    dataflowDefaultReplication = 1;

  @Parameter(names = "--dataflow-tracking-window-size", description="")
  int    dataflowTrackingWindowSize     = 1000;
  
  @Parameter(names = "--dataflow-sliding-window-size", description="")
  int    dataflowSlidingWindowSize      = 15;
  
  @Parameter(names = "--dataflow-num-of-workers", description="")
  int    dataflowNumOfWorker            = 2;
  
  @Parameter(names = "--dataflow-num-of-executor-per-worker", description="")
  int    dataflowNumOfExecutorPerWorker = 7;
}