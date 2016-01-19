package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.message.TrackingWindowReport;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.KafkaWireDataSetFactory;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.vm.VMConfig;

public class TrackingDataflowBuilder {
  static public enum OutputType { kafka, hdfs, s3 }
  
  private String         dataflowId = "tracking";
  private OutputType     outputType = OutputType.kafka;
  private TrackingConfig trackingConfig;
  
  private int numOfWorker            = 2;
  private int numOfExecutorPerWorker = 5;
  private int defaultParallelism            = 5;
  private int defaultReplication            = 1;
  private int trackingWindowSize     = 1000;
  private int slidingWindowSize      = 15;
 
  public TrackingDataflowBuilder(String dataflowId) {
    this.dataflowId = dataflowId;
  
    trackingConfig = new TrackingConfig();
    String dataflowPath = DataflowRegistry.getDataflowPath(dataflowId);
    trackingConfig.setTrackingReportPath(dataflowPath + "/tracking-reports");
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
    
    trackingConfig.setHDFSStorageDir("/storage/hdfs");
  }
  
  public String getDataflowId() { return this.dataflowId; }
  
  public TrackingConfig getTrackingConfig() { return trackingConfig; }
  
  public TrackingDataflowBuilder setHDFSAggregateOutput() {
    this.outputType = OutputType.hdfs;
    return this;
  }
  
  public TrackingDataflowBuilder setMaxRuntime(long maxRuntime) {
    trackingConfig.setValidatorMaxRuntime(maxRuntime);
    return this;
  }
  
  public TrackingDataflowBuilder setTrackingWindowSize(int size) {
    trackingWindowSize = size;
    return this;
  }
  
  public TrackingDataflowBuilder setSlidingWindowSize(int size) {
    slidingWindowSize = size;
    return this;
  }
  
  public TrackingDataflowBuilder setNumOfWorker(int num) {
    numOfWorker = num;
    return this;
  }
  
  public TrackingDataflowBuilder setNumOfExecutorPerWorker(int num) {
    numOfExecutorPerWorker = num;
    return this;
  }
  
  public TrackingDataflowBuilder setDefaultParallelism(int num) {
    defaultParallelism = num;
    return this;
  }
  
  public TrackingDataflowBuilder setDefaultReplication(int num) {
    defaultReplication = num;
    return this;
  }
  
  public Dataflow<TrackingMessage, TrackingMessage> buildDataflow() {
    Dataflow<TrackingMessage, TrackingMessage> dfl = new Dataflow<>(dataflowId);
    dfl.
      useWireDataSetFactory(new KafkaWireDataSetFactory("127.0.0.1:2181")).
      setDefaultParallelism(defaultParallelism).
      setDefaultReplication(defaultReplication).
      setMaxRuntime(trackingConfig.getValidatorMaxRuntime()).
      setTrackingWindowSize(trackingWindowSize).
      setSlidingWindowSize(slidingWindowSize);
    dfl.getWorkerDescriptor().setNumOfInstances(numOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(numOfExecutorPerWorker);
    
    KafkaDataSet<TrackingMessage> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", "127.0.0.1:2181", "tracking.input"));
    
    
    DataSet<TrackingMessage> aggregateDs = null;
    if(outputType == OutputType.hdfs) {
      aggregateDs = 
        dfl.createOutput(new HDFSStorageConfig("aggregate", trackingConfig.getHDFSStorageDir()));
    } else {
      aggregateDs = 
        dfl.createOutput(new KafkaStorageConfig("aggregate", trackingConfig.getKafkaZKConnects(), trackingConfig.getKafkaValidateTopic()));
    }
    Operator<TrackingMessage, TrackingMessage> splitterOp = dfl.createOperator("splitter", TrackingMessageSplitter.class);
    Operator<TrackingMessage, TrackingMessage> infoOp     = dfl.createOperator("info", TrackingMessagePersister.class);
    Operator<TrackingMessage, TrackingMessage> warnOp     = dfl.createOperator("warn", TrackingMessagePersister.class);
    Operator<TrackingMessage, TrackingMessage> errorOp    = dfl.createOperator("error", TrackingMessagePersister.class);

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
  
  public VMConfig buildKafkaVMTMValidator() throws Exception {
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setVmId("vm-"  + dataflowId + "-validator").
      addRoles("vm-" + dataflowId + "-validator").
      withVmApplication(VMTMValidatorKafkaApp.class).
      setVMAppConfig(trackingConfig);
    return vmConfig;
  }
  
  public VMConfig buildHDFSVMTMValidator() throws Exception {
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setVmId("vm-"  + dataflowId + "-validator").
      addRoles("vm-" + dataflowId + "-validator").
      withVmApplication(VMTMValidatorHDFSApp.class).
      setVMAppConfig(trackingConfig);
    return vmConfig;
  }
  
  public void runMonitor(ScribenginShell shell) throws Exception {
    long numOfInputMessages = trackingConfig.getNumOfChunk() * trackingConfig.getNumOfMessagePerChunk();
    ScribenginClient sclient = shell.getScribenginClient();
    DataflowClient dflClient = sclient.getDataflowClient(dataflowId);
    DataflowRegistry dflRegistry = dflClient.getDataflowRegistry();
    
    while(true) {
      TrackingWindowReport report = dflRegistry.getMessageTrackingRegistry().getReport();
      shell.execute(
        "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
        "  --dataflow-id " + dataflowId + " --show-history-workers  --report-path " + trackingConfig.getTrackingReportPath()
      );
      
      System.err.println(
          "numOfInputMessages = " + numOfInputMessages + 
          ", tracking count = " + report.getTrackingCount() +
          ", duplicated = " + report.getTrackingDuplicatedCount());
      if(numOfInputMessages <= report.getTrackingCount()) {
        break;
      }
      
      Thread.sleep(10000);
    }
    
    System.err.println("Should call stop the dataflow here!!!!!!!!!!!");
    shell.execute("dataflow stop --dataflow-id " + dataflowId);

    Thread.sleep(10000);
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
      "  --dataflow-id " + dataflowId + " --show-history-workers  --report-path " + trackingConfig.getTrackingReportPath()
    );
    
    shell.execute(
      "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingJUnitShellPlugin" +
      "  --dataflow-id " + dataflowId + "  --report-path " + trackingConfig.getTrackingReportPath() + " --junit-report-file build/junit-report.xml"
    );
  }
}
