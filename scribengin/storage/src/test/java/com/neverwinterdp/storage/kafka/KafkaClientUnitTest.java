package com.neverwinterdp.storage.kafka;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.zookeeper.ZKClient;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.storage.kafka.KafkaStorage;
import com.neverwinterdp.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class KafkaClientUnitTest {

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testKafkaSink() throws Exception {
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", cluster.getZKConnect());
    KafkaStorage storage = new KafkaStorage(kafkaTool, "writer", "hello");
    KafkaSink sink = (KafkaSink) storage.getSink();
    SinkPartitionStream stream = sink.getPartitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
    }
    writer.close();
    
    ZKClient client = new ZKClient("127.0.0.1:2181");
    client.connect(10000);
    client.dump("/brokers");
    client.close();
    kafkaTool.close();
  }
}
