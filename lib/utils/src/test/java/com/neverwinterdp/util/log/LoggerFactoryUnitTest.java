package com.neverwinterdp.util.log;

import java.net.URL;

import org.junit.Test;
import org.slf4j.Logger;

public class LoggerFactoryUnitTest {
  @Test
  public void testLoggerFactory() throws Exception {
    LoggerFactory.log4jConfigure(new URL("file:src/test/java/com/neverwinterdp/util/log/log4j.properties"));
    LoggerFactory lFactory = new LoggerFactory("[vm=vm-master, app=NeverwinterDP] ");
    Logger logger = lFactory.getLogger(getClass()) ;
    logger.info("This is a test");
    LoggerFactory.log4jConfigure(new URL("file:src/test/java/com/neverwinterdp/util/log/log4j-update.properties"));
    logger.info("This is a test");
  }
}
