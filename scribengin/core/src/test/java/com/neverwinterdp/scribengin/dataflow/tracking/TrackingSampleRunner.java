package com.neverwinterdp.scribengin.dataflow.tracking;

import java.util.Properties;

import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.tracking.VMTMGeneratorKafkaApp;
import com.neverwinterdp.scribengin.dataflow.tracking.VMTMValidatorHDFSApp;
import com.neverwinterdp.scribengin.dataflow.tracking.VMTMValidatorKafkaApp;
import com.neverwinterdp.scribengin.dataflow.tracking.VMTMValidatorS3App;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class TrackingSampleRunner  {
  String REPORT_PATH          = "/applications/tracking-sample/reports";
  String dataflowId           = "tracking-dataflow";
  int    numOfMessagePerChunk = 1000;
  long   dataflowMaxRuntime   = 45000;
  
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  
  public void setup() throws Exception {
    String BASE_DIR = "build/working";
    System.setProperty("app.home", BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster("build/working") ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");
    localScribenginCluster.start();
    
    shell = localScribenginCluster.getShell();
  }
  
  public void teardown() throws Exception {
    localScribenginCluster.shutdown();
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
        "  --prop:kafka.input-topic=tracking.input" +
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

  public void submitKafkaVMTMValidator() throws Exception {
    String storageProps = 
      "  --prop:kafka.zk-connects=127.0.0.1:2181"  +
      "  --prop:kafka.validate-topic=tracking.aggregate"  +
      "  --prop:kafka.message-wait-timeout=30000" ;
    submitVMTMValidator(VMTMValidatorKafkaApp.class, storageProps);
  }
  
  public void submitHDFSTMDataflow() throws Exception {
    String dataflowSubmitCommand = 
        "dataflow submit " + 
        "  --dataflow-config src/test/resources/hdfs-tracking-dataflow.json" +
        "  --dataflow-id " + dataflowId + 
        "  --dataflow-num-of-worker 2 --dataflow-num-of-executor-per-worker 5" + 
        "  --dataflow-max-runtime " + dataflowMaxRuntime;
    shell.execute(dataflowSubmitCommand);
  }
  
  public void submitHDFSVMTMValidator() throws Exception {
    String storageProps = 
      "  --prop:storage.registry.path=/storage/hdfs/tracking-aggregate" + 
      "  --prop:hdfs.partition-roll-period=1200000"  ;
      
    submitVMTMValidator(VMTMValidatorHDFSApp.class, storageProps);
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
  
  public void submitS3VMTMValidator() throws Exception {
    String storageProps = 
      "  --prop:s3.bucket.name=tracking-sample-bucket"  +
      "  --prop:s3.storage.path=aggregate" +
      "  --prop:s3.partition-roll-period=0";
      
    submitVMTMValidator(VMTMValidatorS3App.class, storageProps);
  }
  
  void submitVMTMValidator(Class<?> vmAppType, String storageProps) throws Exception {
    String logValidatorSubmitCommand = 
        "vm submit " +
        "  --dfs-app-home /applications/tracking-sample" +
        "  --registry-connect 127.0.0.1:2181" +
        "  --registry-db-domain /NeverwinterDP" +
        "  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl" + 
        "  --name vm-tracking-validator-1 --role tracking-validator" + 
        
        "  --vm-application " + vmAppType.getName() + 
        
        "  --prop:tracking.report-path=" + REPORT_PATH +
        "  --prop:tracking.num-of-reader=3"  +
        "  --prop:tracking.expect-num-of-message-per-chunk=" + numOfMessagePerChunk +
        "  --prop:tracking.max-runtime=" + dataflowMaxRuntime +
        storageProps ;
    
    shell.execute(logValidatorSubmitCommand);
  }
  
  public void runMonitor() throws Exception {
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
      "  --dataflow-id " + dataflowId + " --show-history-workers " +
      "  --report-path " + REPORT_PATH + " --max-runtime " + dataflowMaxRuntime +"  --print-period 10000"
    );
      
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingJUnitShellPlugin" +
      "  --dataflow-id " + dataflowId + "  --report-path " + REPORT_PATH + " --junit-report-file build/junit-report.xml"
    );
      
    shell.execute("dataflow wait-for-status --dataflow-id "  + dataflowId + " --status TERMINATED") ;
    shell.execute("dataflow info  --dataflow-id " + dataflowId + " --show-tasks --show-workers --show-history-workers ");
    shell.execute("registry dump");
  }
}