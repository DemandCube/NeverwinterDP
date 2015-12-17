package com.neverwinterdp.scribengin.dataflow;


import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingConfig;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingMessage;
import com.neverwinterdp.scribengin.dataflow.tracking.VMTMGeneratorKafkaApp;
import com.neverwinterdp.scribengin.dataflow.tracking.VMTMValidatorKafkaApp;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMSubmitter;

public class TrackingSampleApiRunner  {
  String REPORT_PATH          = "/applications/tracking-sample/reports";
  String dataflowId           = "tracking";
  int    numOfMessagePerChunk = 1000;
  long   dataflowMaxRuntime   = 45000;
  
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  
  public void setup() throws Exception {
    String BASE_DIR = "build/working";
    System.setProperty("app.home", BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    
    shell = localScribenginCluster.getShell();
  }
  
  public void teardown() throws Exception {
    localScribenginCluster.shutdown();
  }
  
  TrackingConfig createTrackingConfig() {
    TrackingConfig vmTrackingConfig = new TrackingConfig();
    vmTrackingConfig.setReportPath(REPORT_PATH);
    vmTrackingConfig.setMessageSize(512);
    vmTrackingConfig.setNumOfChunk(10);
    vmTrackingConfig.setNumOfMessagePerChunk(numOfMessagePerChunk);
    vmTrackingConfig.setGeneratorBreakInPeriod(500);
    vmTrackingConfig.setKafkaZKConnects("127.0.0.1:2181");
    vmTrackingConfig.setKafkaInputTopic("tracking.input");
    vmTrackingConfig.setKafkaValidateTopic("tracking.aggregate");
    vmTrackingConfig.setKafkaNumOfReplication(1);
    vmTrackingConfig.setKafkaNumOfPartition(5);
    return vmTrackingConfig;
  }
  
  public void submitVMTMGenrator() throws Exception {
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setVmId("vm-tracking-generator-1").
      addRoles("vm-tm-generator").
      withVmApplication(VMTMGeneratorKafkaApp.class).
      setRegistryConfig(shell.getVMClient().getRegistry().getRegistryConfig()).
      setVMAppConfig(createTrackingConfig());

    VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), "/applications/tracking-sample", vmConfig);
    vmSubmitter.submit();
    vmSubmitter.waitForRunning(30000);
  }
  
  public void runTMDataflow() throws Exception {
    TrackingDataflowBuilder dflBuilder = new TrackingDataflowBuilder(dataflowId);
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));
    
    new DataflowSubmitter(shell.getScribenginClient(), dfl).
      submit().
      waitForRunning(60000);
  }

  public void submitKafkaVMTMValidator() throws Exception {
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setVmId("vm-tracking-validator-1").
      addRoles("vm-tracking-validator").
      withVmApplication(VMTMValidatorKafkaApp.class).
      setRegistryConfig(shell.getVMClient().getRegistry().getRegistryConfig()).
      setVMAppConfig(createTrackingConfig());

    VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), "/applications/tracking-sample", vmConfig);
    vmSubmitter.submit();
    vmSubmitter.waitForRunning(30000);
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