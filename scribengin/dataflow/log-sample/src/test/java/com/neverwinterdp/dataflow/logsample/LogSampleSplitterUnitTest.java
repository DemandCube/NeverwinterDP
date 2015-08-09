package com.neverwinterdp.dataflow.logsample;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp;
import com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.LoggerConfig;

public class LogSampleSplitterUnitTest  {
  ScribenginClusterBuilder clusterBuilder ;
  Node esNode ;
  ScribenginShell shell;
  
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
    
    ScribenginClient scribenginClient = clusterBuilder.getScribenginClient() ;
    shell = new ScribenginShell(scribenginClient);
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    esNode.close();
  }
  
  @Test
  public void testSplitterToKafka() throws Exception {
    String logGeneratorSubmitCommand = 
        "vm submit " +
        "  --dfs-app-home /applications/log-sample" +
        "  --registry-connect 127.0.0.1:2181" +
        "  --registry-db-domain /NeverwinterDP" +
        "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
        "  --name vm-log-generator-1  --role vm-log-generator" + 
        "  --vm-application " + VMToKafkaLogMessageGeneratorApp.class.getName() + 
        "  --prop:report-path=/applications/log-sample/reports" +
        "  --prop:num-of-message=5000" +
        "  --prop:message-size=512";
    shell.execute(logGeneratorSubmitCommand);
    Thread.sleep(1000);
    
    String dataflowSubmitCommand = 
        "dataflow submit " + 
        "  --dataflow-config src/app/conf/splitter/local/kafka-log-splitter-dataflow.json" +
        "  --dataflow-id log-splitter-dataflow-1";
    shell.execute(dataflowSubmitCommand);
    
    shell.execute(
      "dataflow wait-for-status --dataflow-id log-splitter-dataflow-1 --status TERMINATED"
    );
    shell.execute("dataflow info --dataflow-id log-splitter-dataflow-1 --show-all");
  
    String logValidatorSubmitCommand = 
      "vm submit " +
      "  --dfs-app-home /applications/log-sample" +
      "  --registry-connect 127.0.0.1:2181" +
      "  --registry-db-domain /NeverwinterDP" +
      "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
      "  --name vm-log-validator-1 --role log-validator" + 
      "  --vm-application " + VMLogMessageValidatorApp.class.getName() + 
      "  --prop:report-path=/applications/log-sample/reports" +
      "  --prop:num-of-message-per-partition=5000" +
      "  --prop:wait-for-termination=300000" +
      "  --prop:validate-kafka=log4j.info,log4j.warn,log4j.error";
    shell.execute(logValidatorSubmitCommand);
    shell.execute("vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 25000");
    shell.execute("vm info");
    shell.execute("registry dump --path /applications/log-sample");
  }
}