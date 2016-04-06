package com.neverwinterdp.storage.kafka;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.util.log.LoggerFactory;

public class KafkaConsumeTransactionUnitTest {

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("WARN");
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  @Test
  public void testCommit() throws Exception {
    
  }
}
