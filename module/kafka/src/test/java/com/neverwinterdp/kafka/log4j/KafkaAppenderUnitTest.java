package com.neverwinterdp.kafka.log4j;

import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.Log4jRecord;

public class KafkaAppenderUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }
  
  static private KafkaCluster cluster;

  @BeforeClass
  static public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 2);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }

  @AfterClass
  static public void tearDown() throws Exception {
    cluster.shutdown();
  }

  
  @Test
  public void test() throws Exception {
    FileUtil.removeIfExist("build/buffer", false);
    KafkaAppender appender = new KafkaAppender() ;
    appender.init("127.0.0.1:9092", "log4j", "build/buffer/kafka/log4j");
    appender.activateOptions();
    for(int i = 0; i < 5; i++) {
      Log4jRecord record = new Log4jRecord() ;
      record.setTimestamp(new Date(System.currentTimeMillis()));
      record.setLevel("INFO");
      record.setMessage("message " + i);
      appender.append(record);
    }
    System.out.println("queue done..................");
    Thread.sleep(10000);
    appender.close();
  }
}