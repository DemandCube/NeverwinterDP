package com.neverwinterdp.scribengin.dataflow.log.sample;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Properties;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.os.MemoryInfo;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.sample.log.VMLogMessageValidatorApp;
import com.neverwinterdp.scribengin.dataflow.sample.log.VMToKafkaLogMessageGeneratorApp;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.scribengin.tool.ScribenginClusterBuilder;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class KafkaLogSampleUnitTest  {
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
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    esNode.close();
  }
  
  @Test
  public void testLogSampleChain() throws Exception {
    int NUM_OF_MESSAGE = 25000;
    String REPORT_PATH = "/applications/log-sample/reports";
    String logGeneratorSubmitCommand = 
        "vm submit " +
        "  --dfs-app-home /applications/log-sample" +
        "  --registry-connect 127.0.0.1:2181" +
        "  --registry-db-domain /NeverwinterDP" +
        "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
        "  --name vm-log-generator-1  --role vm-log-generator" + 
        "  --vm-application " + VMToKafkaLogMessageGeneratorApp.class.getName() + 
        "  --prop:report-path=" +    REPORT_PATH +
        "  --prop:num-of-message=" + NUM_OF_MESSAGE +
        "  --prop:num-of-stream=5" +
        "  --prop:message-size=512" +
        "  --prop:send-period=-1";
    shell.execute(logGeneratorSubmitCommand);
    shell.execute(
      "vm wait-for-vm-status --vm-id vm-log-generator-1 --vm-status TERMINATED --max-wait-time 3000"
    );
    
    String dataflowSubmitCommand = 
        "dataflow submit " + 
        "  --dataflow-config src/test/resources/kafka-dataflow-config.json " +
        "  --dataflow-task-switching-period 10000 " +
        "  --dataflow-num-of-worker 2 --dataflow-num-of-executor-per-worker 4";
    shell.execute(dataflowSubmitCommand);
    
    shell.execute(
        "dataflow monitor " +
        "  --dataflow-id log-dataflow --dump-period 5000 " + 
        "  --show-tasks --show-workers --stop-on-status TERMINATED"
    );
    
    shell.execute(
        "dataflow info  --dataflow-id log-dataflow --show-all"
    );
    
    String logValidatorSubmitCommand = 
        "vm submit " +
        "  --dfs-app-home /applications/log-sample" +
        "  --registry-connect 127.0.0.1:2181" +
        "  --registry-db-domain /NeverwinterDP" +
        "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
        "  --name vm-log-validator-1 --role log-validator" + 
        "  --vm-application " + VMLogMessageValidatorApp.class.getName() + 
        "  --prop:report-path=" +                  REPORT_PATH +
        "  --prop:num-of-message-per-partition=" + NUM_OF_MESSAGE +
        "  --prop:wait-for-termination=180000" +
        "  --prop:validate-kafka=log4j.info,log4j.warn,log4j.error";
    shell.execute(logValidatorSubmitCommand);

    shell.execute(
      "vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 45000"
    );
     
    shell.execute("registry dump --path /applications/log-sample");
  }
}