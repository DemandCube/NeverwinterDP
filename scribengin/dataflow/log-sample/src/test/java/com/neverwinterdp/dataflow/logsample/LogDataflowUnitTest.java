package com.neverwinterdp.dataflow.logsample;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.LoggerConfig;

public class LogDataflowUnitTest {
  
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
    
    Logger logger = new LoggerFactory("TEST").getLogger(getClass()) ;
    logger.info("This is an info message test");
    logger.warn("This is a warn message test");
    logger.error("This is an error message test");
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    Thread.sleep(5000);
    
    //LogSampleDataflowBuilder builder = new LogSampleDataflowBuilder(shell.getScribenginClient());
    //DataflowWaitingEventListener listener = builder.submit();
    
    String dflDescriptorJson = IOUtil.getFileContentAsString("src/app/conf/log-splitter-dataflow.json") ;
    DataflowDescriptor dflDescriptor = JSONSerializer.INSTANCE.fromString(dflDescriptorJson, DataflowDescriptor.class) ;
    runDataflow(dflDescriptor);
    
    LogPersisterDataflowBuilder infoPersisterBuilder = new LogPersisterDataflowBuilder("log4j.info");
    runDataflow(infoPersisterBuilder.getDataflowDescriptor());
  }
  
  void runDataflow(DataflowDescriptor dflDescriptor) throws Exception {
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
    DataflowSubmitter submitter = new DataflowSubmitter(shell.getScribenginClient(), null, dflDescriptor) ;
    try { 
      submitter.submitAndWaitForTerminatedStatus(60000);
    } catch(Exception ex) {
      ex.printStackTrace();
      shell.execute("registry dump");
    }
  }
}