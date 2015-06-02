package com.neverwinterdp.dataflow.logsample;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.LoggerConfig;

public class LogSampleDataflowUnitTest {
  
  protected ScribenginClusterBuilder clusterBuilder ;
  protected ScribenginShell shell;

  @BeforeClass
  static public void init() throws Exception {
    FileUtil.removeIfExist("build/data", false);
    FileUtil.removeIfExist("build/logs", false);
    LoggerConfig loggerConfig = new LoggerConfig() ;
    
    loggerConfig.getConsoleAppender().setEnable(false);
    loggerConfig.getFileAppender().initLocalEnvironment();
    loggerConfig.getEsAppender().initLocalEnvironment();
    loggerConfig.getKafkaAppender().initLocalEnvironment();
    LoggerFactory.log4jConfigure(loggerConfig.getLog4jConfiguration());
  }
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    
//    Logger logger = new LoggerFactory("TEST").getLogger(getClass()) ;
//    logger.info("This is an info message test");
//    logger.warn("This is a warn message test");
//    logger.error("This is an error message test");
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void testLogSampleDataflow() throws Exception {
    Thread.sleep(5000);
    LogSampleDataflowBuilder builder = new LogSampleDataflowBuilder(shell.getScribenginClient());
    DataflowWaitingEventListener listener = builder.submit();
    try { 
      listener.getWaitingNodeEventListener().waitForEvents(60 * 1000);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
    System.out.println(listener.getTabularFormaterEventLogInfo().getFormatText()); 
  }
}