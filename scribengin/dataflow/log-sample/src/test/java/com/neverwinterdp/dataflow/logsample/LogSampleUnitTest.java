package com.neverwinterdp.dataflow.logsample;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.LoggerConfig;

public class LogSampleUnitTest  {
  ScribenginClusterBuilder clusterBuilder ;
  Node esNode ;
  
  @BeforeClass
  static public void init() throws Exception {
    FileUtil.removeIfExist("build/hdfs", false);
    FileUtil.removeIfExist("build/data", false);
    FileUtil.removeIfExist("build/logs", false);
    FileUtil.removeIfExist("build/elasticsearch", false);
    FileUtil.removeIfExist("build/cluster", false);
    
    LoggerConfig loggerConfig = new LoggerConfig() ;
    loggerConfig.getConsoleAppender().setEnable(false);
    loggerConfig.getFileAppender().initLocalEnvironment();
    //loggerConfig.getEsAppender().initLocalEnvironment();
    loggerConfig.getKafkaAppender().initLocalEnvironment();
    LoggerFactory.log4jConfigure(loggerConfig.getLog4jConfiguration());
  }
  
  @Before
  public void setup() throws Exception {
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          "build/elasticsearch/data");
    nb.getSettings().put("node.name",          "elasticsearch-1");
    nb.getSettings().put("transport.tcp.port", "9300");
    esNode = nb.node();
    
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    esNode.close();
  }
  
  @Test
  public void testLogSample() throws Exception {
    String[] args = {
      "--registry-connect", "127.0.0.1:2181",
      "--registry-db-domain", "/NeverwinterDP",
      "--registry-implementation", RegistryImpl.class.getName(),
      
      "--log-generator-num-of-vm", "2",
      "--log-generator-num-of-executor-per-vm", "2",
      "--log-generator-num-of-message-per-executor", "3000",
      "--log-generator-message-size", "128",
      
      "--log-validator-num-of-executor-per-vm", "3",
      "--log-validator-wait-for-message-timeout", "5000",
      "--log-validator-wait-for-termination", "30000",
      
      "--dataflow-descriptor", "src/app/conf/local/log-dataflow-chain.json",
      "--dataflow-task-debug"
    } ;
    LogSampleRunner.main(args);
  }
}