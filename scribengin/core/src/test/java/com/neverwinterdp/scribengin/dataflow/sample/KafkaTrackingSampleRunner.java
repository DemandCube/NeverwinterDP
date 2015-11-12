package com.neverwinterdp.scribengin.dataflow.sample;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Properties;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.VMTMGeneratorKafkaApp;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.VMTMValidatorKafkaApp;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.scribengin.tool.ScribenginClusterBuilder;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class KafkaTrackingSampleRunner  {
  String REPORT_PATH          = "/applications/tracking-sample/reports";
  String dataflowId           = "tracking-dataflow";
  int    numOfMessagePerChunk = 1000;
  long   dataflowMaxRuntime   = 45000;
  
  ScribenginClusterBuilder clusterBuilder ;
  Node esNode ;
  ScribenginShell shell;
  
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/hdfs", false);
    FileUtil.removeIfExist("build/data", false);
    FileUtil.removeIfExist("build/logs", false);
    FileUtil.removeIfExist("build/elasticsearch", false);
    FileUtil.removeIfExist("build/cluster", false);
    FileUtil.removeIfExist("build/scribengin", false);
    
    System.setProperty("vm.app.dir", "build/scribengin");
    Properties log4jProps = new Properties() ;
    log4jProps.load(IOUtil.loadRes("classpath:scribengin/log4j/vm-log4j.properties"));
    log4jProps.setProperty("log4j.rootLogger", "INFO, file");
    LoggerFactory.log4jConfigure(log4jProps);
    
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          "build/elasticsearch/data");
    nb.getSettings().put("node.name",          "elasticsearch-1");
    nb.getSettings().put("transport.tcp.port", "9300");
    esNode = nb.node();
    
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    
    ScribenginClient scribenginClient = clusterBuilder.getScribenginClient() ;
    shell = new ScribenginShell(scribenginClient);
  }
  
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    esNode.close();
  }
  
  public void submitVMTMGenrator() throws Exception {
    String logGeneratorSubmitCommand = 
        "vm submit " +
        "  --dfs-app-home /applications/tracking-sample" +
        "  --registry-connect 127.0.0.1:2181" +
        "  --registry-db-domain /NeverwinterDP" +
        "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
        "  --name vm-tracking-generator-1 --role vm-tm-generator" + 
        "  --vm-application " + VMTMGeneratorKafkaApp.class.getName() + 
        
        "  --prop:tracking.report-path=" + REPORT_PATH +
        "  --prop:tracking.num-of-writer=1" +
        "  --prop:tracking.num-of-chunk=10" +
        "  --prop:tracking.num-of-message-per-chunk=" + numOfMessagePerChunk +
        "  --prop:tracking.break-in-period=500" +
        "  --prop:tracking.message-size=512" +
         
        "  --prop:kafka.zk-connects=127.0.0.1:2181" +
        "  --prop:kafka.topic=tracking.input" +
        "  --prop:kafka.num-of-partition=5" +
        "  --prop:kafka.replication=1" ;
    shell.execute(logGeneratorSubmitCommand);
  }
  
  public void submitKafkaTMDataflow() throws Exception {
    String dataflowChainSubmitCommand = 
        "dataflow submit " + 
        "  --dataflow-config src/test/resources/kafka-tracking-dataflow.json" +
        "  --dataflow-id " + dataflowId + 
        "  --dataflow-num-of-worker 2 --dataflow-num-of-executor-per-worker 5" + 
        "  --dataflow-max-runtime " + dataflowMaxRuntime;
    shell.execute(dataflowChainSubmitCommand);
  }
  
  public void submitS3TMDataflow() throws Exception {
    String dataflowSubmitCommand = 
        "dataflow submit " + 
        "  --dataflow-config src/test/resources/s3-tracking-dataflow.json" +
        "  --dataflow-id " + dataflowId + 
        "  --dataflow-num-of-worker 2 --dataflow-num-of-executor-per-worker 5" + 
        "  --dataflow-max-runtime " + dataflowMaxRuntime;
    shell.execute(dataflowSubmitCommand);
  }
  
  public void submitKafkaVMTMValidator() throws Exception {
    String logValidatorSubmitCommand = 
        "vm submit " +
        "  --dfs-app-home /applications/tracking-sample" +
        "  --registry-connect 127.0.0.1:2181" +
        "  --registry-db-domain /NeverwinterDP" +
        "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
        "  --name vm-tracking-validator-1 --role tracking-validator" + 
        
        "  --vm-application " + VMTMValidatorKafkaApp.class.getName() + 
        
        "  --prop:tracking.report-path=" + REPORT_PATH +
        "  --prop:tracking.num-of-reader=3"  +
        "  --prop:tracking.expect-num-of-message-per-chunk=" + numOfMessagePerChunk +
        "  --prop:tracking.max-runtime=120000"  +
        
        "  --prop:kafka.zk-connects=127.0.0.1:2181"  +
        "  --prop:kafka.topic=tracking.aggregate"  +
        "  --prop:kafka.message-wait-timeout=30000" ;
    shell.execute(logValidatorSubmitCommand);
  }
  
  public void runMonitor() throws Exception {
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMonitor" +
      "  --dataflow-id " + dataflowId + " --show-history-workers " +
      "  --report-path " + REPORT_PATH + " --max-runtime " + dataflowMaxRuntime +"  --print-period 10000"
    );
      
    shell.execute(
        "plugin com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingJUnitShellPlugin" +
        "  --dataflow-id " + dataflowId + "  --report-path " + REPORT_PATH + " --junit-report-file build/junit-report.xml"
      );
      
    shell.execute("dataflow wait-for-status --dataflow-id "  + dataflowId + " --status TERMINATED") ;
    shell.execute("dataflow info  --dataflow-id " + dataflowId + " --show-tasks --show-workers --show-history-workers ");
    shell.execute("registry dump");
  }
}