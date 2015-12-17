package com.neverwinterdp.scribengin.dataflow.api;


import java.util.Properties;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.sample.TrackingMessagePerister;
import com.neverwinterdp.scribengin.dataflow.sample.TrackingMessageSplitter;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.VMTMGeneratorKafkaApp;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.VMTMValidatorKafkaApp;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.LocalScribenginCluster;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class TrackingSampleApiRunner  {
  String REPORT_PATH          = "/applications/tracking-sample/reports";
  String dataflowId           = "tracking";
  int    numOfMessagePerChunk = 1000;
  long   dataflowMaxRuntime   = 45000;
  
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/working", false);
    FileUtil.removeIfExist("build/data", false);
    FileUtil.removeIfExist("build/logs", false);
    FileUtil.removeIfExist("build/cluster", false);
    FileUtil.removeIfExist("build/scribengin", false);
    
    System.setProperty("vm.app.dir", "build/scribengin");
    Properties log4jProps = new Properties() ;
    log4jProps.load(IOUtil.loadRes("classpath:scribengin/log4j/vm-log4j.properties"));
    log4jProps.setProperty("log4j.rootLogger", "INFO, file");
    LoggerFactory.log4jConfigure(log4jProps);
    
    localScribenginCluster = new LocalScribenginCluster("build/working") ;
    localScribenginCluster.clean(); 
    localScribenginCluster.start();
    
    ScribenginClient scribenginClient = localScribenginCluster.getScribenginClient() ;
    shell = new ScribenginShell(scribenginClient);
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
        "  --prop:kafka.topic=tracking.input" +
        "  --prop:kafka.num-of-partition=5" +
        "  --prop:kafka.replication=1" ;
    shell.execute(logGeneratorSubmitCommand);
  }
  
  public void runTMDataflow() throws Exception {
    Dataflow<Message, Message> dfl = new Dataflow<Message, Message>(dataflowId);
    dfl.
      useWireDataSetFactory(new KafkaWireDataSetFactory("127.0.0.1:2181")).
      setDefaultParallelism(5).
      setDefaultReplication(1).
      setMaxRuntime(dataflowMaxRuntime);
    dfl.getWorkerDescriptor().setNumOfInstances(2);
    dfl.getWorkerDescriptor().setNumOfExecutor(5);
    
    KafkaDataSet<Message> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", "127.0.0.1:2181", "tracking.input"));
    KafkaDataSet<Message> aggregateDs = 
        dfl.createOutput(new KafkaStorageConfig("aggregate", "127.0.0.1:2181", "tracking.aggregate"));
   
    Operator<Message, Message> splitterOp = dfl.createOperator("splitter", TrackingMessageSplitter.class);
    Operator<Message, Message> infoOp     = dfl.createOperator("info", TrackingMessagePerister.class);
    Operator<Message, Message> warnOp     = dfl.createOperator("warn", TrackingMessagePerister.class);
    Operator<Message, Message> errorOp    = dfl.createOperator("error", TrackingMessagePerister.class);

    inputDs.
      useRawReader().
      connect(splitterOp);
    splitterOp.
      connect(infoOp).
      connect(warnOp).
      connect(errorOp);
    
    infoOp.connect(aggregateDs);
    warnOp.connect(aggregateDs);
    errorOp.connect(aggregateDs);
    
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));
    
    DataflowSubmitter submitter = new DataflowSubmitter(shell.getScribenginClient(), dflDescriptor);
    submitter.submit();
    submitter.waitForRunning(60000);
  }

  public void submitKafkaVMTMValidator() throws Exception {
    String storageProps = 
      "  --prop:kafka.zk-connects=127.0.0.1:2181"  +
      "  --prop:kafka.topic=tracking.aggregate"  +
      "  --prop:kafka.message-wait-timeout=30000" ;
    submitVMTMValidator(VMTMValidatorKafkaApp.class, storageProps);
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