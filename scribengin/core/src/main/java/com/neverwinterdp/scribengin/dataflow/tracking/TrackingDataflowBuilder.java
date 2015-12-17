package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.KafkaWireDataSetFactory;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.vm.VMConfig;

public class TrackingDataflowBuilder {
  private String dataflowId           = "tracking";
  private TrackingConfig trackingConfig;
  
  public TrackingDataflowBuilder(String dataflowId) {
    this.dataflowId = dataflowId;
  
    trackingConfig = new TrackingConfig();
    trackingConfig.setReportPath("/applications/tracking-sample/reports");
    trackingConfig.setMessageSize(512);
    trackingConfig.setNumOfChunk(10);
    trackingConfig.setNumOfMessagePerChunk(1000);
    
    trackingConfig.setGeneratorBreakInPeriod(500);
    
    trackingConfig.setValidatorMaxRuntime(90000);
    
    trackingConfig.setKafkaZKConnects("127.0.0.1:2181");
    trackingConfig.setKafkaInputTopic("tracking.input");
    trackingConfig.setKafkaValidateTopic("tracking.aggregate");
    trackingConfig.setKafkaNumOfReplication(1);
    trackingConfig.setKafkaNumOfPartition(5);
  }
  
  
  
  public Dataflow<TrackingMessage, TrackingMessage> buildDataflow() {
    Dataflow<TrackingMessage, TrackingMessage> dfl = new Dataflow<>(dataflowId);
    dfl.
      useWireDataSetFactory(new KafkaWireDataSetFactory("127.0.0.1:2181")).
      setDefaultParallelism(5).
      setDefaultReplication(1).
      setMaxRuntime(trackingConfig.getValidatorMaxRuntime());
    dfl.getWorkerDescriptor().setNumOfInstances(2);
    dfl.getWorkerDescriptor().setNumOfExecutor(5);
    
    KafkaDataSet<TrackingMessage> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", "127.0.0.1:2181", "tracking.input"));
    KafkaDataSet<TrackingMessage> aggregateDs = 
        dfl.createOutput(new KafkaStorageConfig("aggregate", "127.0.0.1:2181", "tracking.aggregate"));
   
    Operator<TrackingMessage, TrackingMessage> splitterOp = dfl.createOperator("splitter", TrackingMessageSplitter.class);
    Operator<TrackingMessage, TrackingMessage> infoOp     = dfl.createOperator("info", TrackingMessagePerister.class);
    Operator<TrackingMessage, TrackingMessage> warnOp     = dfl.createOperator("warn", TrackingMessagePerister.class);
    Operator<TrackingMessage, TrackingMessage> errorOp    = dfl.createOperator("error", TrackingMessagePerister.class);

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
    return dfl;
  }
  
  public VMConfig buildVMTMGeneratorKafka() throws Exception {
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setVmId("vm-" + dataflowId + "-generator").
      addRoles("vm-" + dataflowId + "-generator").
      withVmApplication(VMTMGeneratorKafkaApp.class).
      setVMAppConfig(trackingConfig);
    return vmConfig;
  }
  
  public VMConfig buildKafkaVMTMValidatorKafka() throws Exception {
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setVmId("vm-"  + dataflowId + "-validator").
      addRoles("vm-" + dataflowId + "-validator").
      withVmApplication(VMTMValidatorKafkaApp.class).
      setVMAppConfig(trackingConfig);
    return vmConfig;
  }
  
  
  public void runMonitor(ScribenginShell shell) throws Exception {
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
      "  --dataflow-id " + dataflowId + " --show-history-workers " +
      "  --report-path " + trackingConfig.getReportPath() + " --max-runtime " + trackingConfig.getValidatorMaxRuntime() +"  --print-period 10000"
    );
      
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingJUnitShellPlugin" +
      "  --dataflow-id " + dataflowId + "  --report-path " + trackingConfig.getReportPath() + " --junit-report-file build/junit-report.xml"
    );
      
    shell.execute("dataflow wait-for-status --dataflow-id "  + dataflowId + " --status TERMINATED") ;
    shell.execute("dataflow info  --dataflow-id " + dataflowId + " --show-tasks --show-workers --show-history-workers ");
    shell.execute("registry dump");
  }
}
