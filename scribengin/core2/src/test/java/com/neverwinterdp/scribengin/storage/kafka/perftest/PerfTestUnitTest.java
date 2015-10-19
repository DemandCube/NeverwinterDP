package com.neverwinterdp.scribengin.storage.kafka.perftest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.scribengin.storage.kafka.perftest.KafkaPerfTest;

public class PerfTestUnitTest {
  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.setNumOfPartition(5);
    cluster.start();
    Thread.sleep(3000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testPerf() throws Exception {
    String[] args = {
        "--topics", "tracking.input,tracking.info,tracking.warn,tracking.error,tracking.aggregate",

        "--zk-connect", "127.0.0.1:2181",
        "--topic-num-of-message",     "500000",
        "--topic-num-of-partition",   "10",
        "--topic-num-of-replication", "1",
        
        "--writer-write-per-writer", "1000",
        "--writer-write-break-in-period", "25",
        
        "--reader-read-per-reader", "1000",
        "--reader-run-delay", "5000",
        "--max-runtime", "600000"
    };
    KafkaPerfTest.main(args);
  }
}
