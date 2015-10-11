package com.neverwinterdp.scribengin.storage.kafka.test;

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
        "--zk-connect", "127.0.0.1:2181",
        
        "--topic", "pertest",
        "--topic-num-of-message", "10000",
        "--topic-num-of-partition", "10",
        "--topic-num-of-replication", "1",
        
        "--writer-write-per-writer", "1000",
        
        "--reader-read-per-reader", "1000"
    };
    KafkaPerfTest.main(args);
  }
}
