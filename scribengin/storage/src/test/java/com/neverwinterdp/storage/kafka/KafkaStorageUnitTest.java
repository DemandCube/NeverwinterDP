package com.neverwinterdp.storage.kafka;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.kafka.KafkaStorage;
import com.neverwinterdp.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.storage.kafka.source.KafkaSource;
import com.neverwinterdp.storage.kafka.source.KafkaSourcePartition;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.util.log.LoggerFactory;

public class KafkaStorageUnitTest {
  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("WARN");
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
  public void testKafkaSource() throws Exception {
    String zkConnect = cluster.getZKConnect();
    System.out.println("zkConnect = " + zkConnect);
    String TOPIC = "hello.topic" ;
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnect);
    KafkaStorage storage = new KafkaStorage(kafkaTool, "hello", TOPIC);
    KafkaSink sink = (KafkaSink) storage.getSink();
    
    SinkPartitionStream stream = sink.getPartitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
      String hello = "Hello " + i ;
      Message dataflowMessage = new Message("key-" + i, hello.getBytes());
      writer.append(dataflowMessage);
    }
    writer.close();
    
    KafkaSource source = new KafkaSource(kafkaTool, "hello", TOPIC);
    KafkaSourcePartition partition = (KafkaSourcePartition) source.getLatestSourcePartition();
    SourcePartitionStream[] streams = partition.getPartitionStreams();
    Assert.assertEquals(5, streams.length);
    for(int i = 0; i < streams.length; i++) {
      System.out.println("Stream id: " + streams[i].getPartitionStreamConfig().getPartitionStreamId());
      SourcePartitionStreamReader reader = streams[i].getReader("kafka");
      Message message = null;
      while((message = reader.next(1000)) != null) {
        System.out.println("Record: " + new String(message.getData()));
      }
    }
  }
}
