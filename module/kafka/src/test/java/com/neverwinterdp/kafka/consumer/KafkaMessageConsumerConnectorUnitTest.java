package com.neverwinterdp.kafka.consumer;


import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.util.io.FileUtil;

public class KafkaMessageConsumerConnectorUnitTest {
  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    FileUtil.removeIfExist("./build/cluster", false);
    
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.setNumOfPartition(5);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testReader() throws Exception {
    String NAME = "test";
    String TOPIC = "hello";
    DefaultKafkaWriter writer = new DefaultKafkaWriter(NAME, cluster.getKafkaConnect());
    for(int i = 0; i < 100; i++) {
      String hello = "Hello " + i;
      writer.send(TOPIC, "key-" + i, hello, 5000);
    }
    writer.close();
    
    KafkaMessageConsumerConnector connector = 
        new KafkaMessageConsumerConnector(NAME, cluster.getZKConnect()).
        withConsumerTimeoutMs(1000).
        connect();
   
    MessageConsumerHandler handler = new MessageConsumerHandler() {
      @Override
      public void onMessage(String topic, byte[] key, byte[] message) {
        String mesg = new String(message);
        System.out.println("on message: " + mesg);
      }
    };
    connector.consume(TOPIC, handler, 1);
    connector.awaitTermination(5000, TimeUnit.MILLISECONDS);
  }
}
