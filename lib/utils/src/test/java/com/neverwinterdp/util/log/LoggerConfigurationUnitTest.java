package com.neverwinterdp.util.log;

import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;

import com.neverwinterdp.util.JSONSerializer;

public class LoggerConfigurationUnitTest {
  @Test
  public void test() throws Exception {
    LoggerConfiguration conf = new LoggerConfiguration() ;
    conf.createFileAppender("file", "build/test.log") ;
    conf.createLogger("rootLogger", "INFO", "file");
    Map<String, String> log4jProps = conf.getLog4jConfiguration() ;
    System.out.println(JSONSerializer.INSTANCE.toString(log4jProps));
    
    LoggerFactory lFactory = new LoggerFactory("[TEST] ");
    LoggerFactory.log4jConfigure(log4jProps);
    Logger logger = lFactory.getLogger(getClass()) ;
    logger.info("This is a test");
  }
}
