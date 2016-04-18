package com.neverwinterdp.analytics;

import com.beust.jcommander.Parameter;

public class AnalyticsConfig {
  @Parameter(names = {"--help", "-h"}, help = true, description = "Output this help message")
  public boolean help;

  @Parameter(names = "--local-app-home", required=true, description="The example dataflow local location")
  public String localAppHome ;
  
  @Parameter(names = "--dfs-app-home", description="DFS location to upload the example dataflow")
  public String dfsAppHome = "/applications/dataflow/analytics";
  
  @Parameter(names = "--zk-connect", description="[host]:[port] of Zookeeper server")
  public String zkConnect = "localhost:2181";
  
  @Parameter(names = "--es-addresses", description="Elasticsearch addresses, ex: elasticsearch-1:9300,elasticsearch-2:9300")
  public String esAddresses = "localhost:9300";
  
  @Parameter(names = "--hadoop-master-connect", description="Hostname of HadoopMaster")
  public String hadoopMasterConnect = "hadoop-master";

  @Parameter(names = "--generator-odyssey-input-topic", description="")
  public String generatorOdysseyInputTopic = "odyssey.input";
  
  @Parameter(names = "--generator-odyssey-num-of-events", description="")
  public int generatorOdysseyNumOfEvents = 100000;
  
  @Parameter(names = "--generator-web-input-topic", description="")
  public String dataflowWebInputTopic = "web.input";
  
  @Parameter(names = "--generator-web-num-of-threads", description="")
  public int generatorWebNumOfThreads = 1;
  
  @Parameter(names = "--generator-web-thread-sleep", description="")
  public long generatorWebThreadSleep = 1;
  
  @Parameter(names = "--generator-web-num-of-events", description="")
  public int generatorWebNumOfEvents = 100000;
  
  @Parameter(names = "--generator-web-max-visit-time", description="Max Visit Time")
  public int generatorWebMaxVisitTime = 0;
  
  @Parameter(names = "--generator-web-min-visit-time", description="Max Visit Time")
  public int generatorWebMinVisitTime = 0;
  
  @Parameter(names = "--dataflow-id", description="")
  public String dataflowId         = "analytics";
  
  @Parameter(names = "--dataflow-default-parallelism", description="")
  public int    dataflowDefaultParallelism = 5;
  
  @Parameter(names = "--dataflow-default-replication", description="")
  public int    dataflowDefaultReplication = 1;

  @Parameter(names = "--dataflow-tracking-window-size", description="")
  public int    dataflowTrackingWindowSize     = 1000;
  
  @Parameter(names = "--dataflow-sliding-window-size", description="")
  public int    dataflowSlidingWindowSize      = 15;
  
  @Parameter(names = "--dataflow-num-of-workers", description="")
  public int    dataflowNumOfWorker            = 3;
  
  @Parameter(names = "--dataflow-num-of-executor-per-worker", description="")
  public int    dataflowNumOfExecutorPerWorker = 6;
  
  @Parameter(names = "--dataflow-ads-input-topic", description="")
  public String dataflowADSInputTopic = "ads.input";
  
  @Parameter(names = "--wait-for-input", description="")
  public boolean waitForInput = false;
}