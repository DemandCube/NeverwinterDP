package com.neverwinterdp.kafka.log4j;

import org.junit.Test;

import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.log.Log4jRecord;

public class KafkaAppenderUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }
  
  @Test
  public void test() throws Exception {
    FileUtil.removeIfExist("build/buffer", false);
    KafkaAppender appender = new KafkaAppender() ;
    appender.init("127.0.0.1:9092", "log4j", "build/buffer/kafka/log4j");
    appender.activateOptions();
    for(int i = 0; i < 5; i++) {
      Log4jRecord record = new Log4jRecord() ;
      record.setTimestamp(System.currentTimeMillis());
      record.setLevel("INFO");
      record.setMessage("message " + i);
      appender.append(record);
    }
    System.out.println("queue done..................");
    Thread.sleep(10000);
  }
}