package com.neverwinterdp.dataflow.logsample;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.chain.DataflowChainConfig;
import com.neverwinterdp.scribengin.dataflow.chain.OrderDataflowChainSubmitter;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.LoggerConfig;

public class LogDataflowChainUnitTest {
  
  ScribenginClusterBuilder clusterBuilder ;
  Node esNode ;
  ScribenginShell shell;
  
  @BeforeClass
  static public void init() throws Exception {
    FileUtil.removeIfExist("build/data", false);
    FileUtil.removeIfExist("build/logs", false);
    FileUtil.removeIfExist("build/elasticsearch", false);
    FileUtil.removeIfExist("build/buffer", false);
    
    LoggerConfig loggerConfig = new LoggerConfig() ;
    loggerConfig.getConsoleAppender().setEnable(false);
    loggerConfig.getFileAppender().initLocalEnvironment();
    //loggerConfig.getEsAppender().initLocalEnvironment();
    loggerConfig.getKafkaAppender().initLocalEnvironment();
    LoggerFactory.log4jConfigure(loggerConfig.getLog4jConfiguration());
  }
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    
    
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          "build/elasticsearch/data");
    nb.getSettings().put("node.name",          "elasticsearch-1");
    nb.getSettings().put("transport.tcp.port", "9300");
    esNode = nb.node();
    
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    
    Logger logger = new LoggerFactory("TEST").getLogger(getClass()) ;
    logger.info("This is an info message test");
    logger.warn("This is a warn message test");
    logger.error("This is an error message test");
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    esNode.close();
  }
  
  @Test
  public void test() throws Exception {
    new LogGenerator().start();
    Thread.sleep(15000);
    
    String json = IOUtil.getFileContentAsString("src/app/conf/log-dataflow-chain.json") ;
    DataflowChainConfig config = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
    OrderDataflowChainSubmitter submitter = 
        new OrderDataflowChainSubmitter(shell.getScribenginClient(), null, config);
    submitter.submit(30000);
    submitter.waitForTerminated(60000);
    shell.execute("dataflow info --dataflow-id log-splitter-dataflow-1") ;
    shell.execute("dataflow info --dataflow-id info-log-persister-dataflow-info") ;
    shell.execute("dataflow info --dataflow-id info-log-persister-dataflow-warn") ;
    shell.execute("dataflow info --dataflow-id info-log-persister-dataflow-error") ;
  }
  
  public class LogGenerator extends Thread {
    public void run() {
      Logger logger = new LoggerFactory("[TEST]").getLogger("LogGenerator");
      try {
        for(int i = 0; i < 1000; i++) {
          logger.info("this is an info " + i);
          logger.warn("this is a warning " + i);
          logger.error("this is an error " + i);
          Thread.sleep(5);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}