package com.neverwinterdp.dataflow.logsample.chain;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.dataflow.logsample.chain.LogSampleChainClient;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.LoggerConfig;

public class LogSampleUnitTest  {
  ScribenginClusterBuilder clusterBuilder ;
  Node esNode ;
  
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/hdfs", false);
    FileUtil.removeIfExist("build/data", false);
    FileUtil.removeIfExist("build/logs", false);
    FileUtil.removeIfExist("build/elasticsearch", false);
    FileUtil.removeIfExist("build/cluster", false);
    
    LoggerConfig loggerConfig = new LoggerConfig() ;
    loggerConfig.getConsoleAppender().setEnable(false);
    loggerConfig.getFileAppender().initLocalEnvironment();
    //loggerConfig.getEsAppender().initLocalEnvironment();
    //loggerConfig.getKafkaAppender().initLocalEnvironment();
    LoggerFactory.log4jConfigure(loggerConfig.getLog4jConfiguration());
    
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
  public void testKafka() throws Exception {
    String[] args = {
        "--registry-connect", "127.0.0.1:2181",
        "--registry-db-domain", "/NeverwinterDP",
        "--registry-implementation", RegistryImpl.class.getName(),
        
        "--log-generator-num-of-vm", "1",
        "--log-generator-num-of-message", "5000",
        "--log-generator-message-size", "128",
        
        "--dataflow-descriptor", "src/app/conf/chain/local/kafka-log-dataflow-chain.json",
        "--dataflow-task-dedicated-executor", "false",
        "--dataflow-wait-for-submit-timeout", "45000",
        "--dataflow-wait-for-termination-timeout", "90000",
        "--dataflow-task-debug",
        
        "--log-validator-wait-for-termination", "45000",
        "--log-validator-validate-kafka", "log4j.aggregate"
      } ;
    
    LogSampleChainClient client = new LogSampleChainClient(args);
    client.run();
  }
  
  @Test
  public void testHDFS() throws Exception {
    String[] args = {
        "--registry-connect", "127.0.0.1:2181",
        "--registry-db-domain", "/NeverwinterDP",
        "--registry-implementation", RegistryImpl.class.getName(),
        
        "--log-generator-num-of-vm", "1",
        "--log-generator-num-of-message", "5000",
        "--log-generator-message-size", "128",
        
        "--dataflow-descriptor", "src/app/conf/chain/local/hdfs-log-dataflow-chain.json",
        "--dataflow-wait-for-submit-timeout", "45000",
        "--dataflow-wait-for-termination-timeout", "90000",
        "--dataflow-task-debug",
        
        "--log-validator-wait-for-termination", "90000",
        "--log-validator-validate-hdfs", "./build/hdfs/info,./build/hdfs/warn,./build/hdfs/error"
    } ;
    LogSampleChainClient client = new LogSampleChainClient(args);
    client.run();
  }
  
  
  @Test
  public void testS3() throws Exception {
    S3Client s3Client = new S3Client();
    if(s3Client.hasBucket("test-log-sample")) {
      s3Client.deleteBucket("test-log-sample", true);
    }
    s3Client.createBucket("test-log-sample");
    s3Client.onDestroy();
    String[] args = {
        "--registry-connect", "127.0.0.1:2181",
        "--registry-db-domain", "/NeverwinterDP",
        "--registry-implementation", RegistryImpl.class.getName(),
        
        "--log-generator-num-of-vm", "1",
        "--log-generator-num-of-message", "1000",
        "--log-generator-message-size", "128",
        
        "--dataflow-descriptor", "src/app/conf/chain/local/s3-log-dataflow-chain.json",
        "--dataflow-wait-for-submit-timeout", "45000",
        "--dataflow-wait-for-termination-timeout", "120000",
        "--dataflow-task-debug",
        
        "--log-validator-wait-for-termination", "45000",
        "--log-validator-validate-s3", "test-log-sample:info,test-log-sample:warn,test-log-sample:error"
    } ;
    
    LogSampleChainClient.main(args);
  }
}