package com.neverwinterdp.scribengin.dataflow.tracking;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.VMSubmitter;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class TrackingLauncher  extends SubCommand {
  @Parameter(names = "--local-app-home", required = true, description = "Generator num of chunk")
  String localAppHome        = null;
  
  @Parameter(names = "--dfs-app-home", description = "Generator num of chunk")
  String dfsAppHome  = "/applications/tracking-sample";
  
  @Parameter(names = "--generator-num-of-chunk", description = "Generator num of chunk")
  private int generatorNumOfChunks          = 10;
  
  @Parameter(names = "--generator-num-of-message-per-chunk", description = "")
  private int generatorNumOfMessagePerChunk = 1000;
  
  @Parameter(names = "--generator-num-of-writer", description = "")
  private int generatorNumOfWriter = 3;
  
  @Parameter(names = "--generator-break-in-period", description = "")
  private long generatorBreakInPeriod = 50;
  
  @Parameter(names = "--generator-kafka-num-of-partition", description = "")
  private int generatorKafkaNumOfPartition = 8;
  
  @Parameter(names = "--generator-kafka-num-of-replication", description = "")
  private int generatorKafkaNumOfReplication = 2;
  
  @Parameter(names = "--dataflow-id", description = "")
  private String dataflowId        = "tracking";
  
  @Parameter(names = "--dataflow-storage", description = "")
  private String dataflowStorage   = "kafka";
  
  @Parameter(names = "--dataflow-num-of-worker", description = "")
  private int numOfWorker = 8;
  
  @Parameter(names = "--dataflow-num-of-executor-per-worker", description = "")
  private int numOfExecutorPerWorker = 2;
  
  @Parameter(names = "--dataflow-default-parallelism", description = "")
  int dataflowDefaultParallelism = 8;
  
  @Parameter(names = "--dataflow-default-replication", description = "")
  int dataflowDefaultReplication = 2;
  
  @Parameter(names = "--dataflow-tracking-window-size", description = "")
  private int trackingWindowSize = 1000;
  
  @Parameter(names = "--dataflow-sliding-window-size", description = "")
  private int slidingWindowSize = 100;
  
  @Parameter(names = "--validator-num-of-reader", description = "")
  private int validatorNumOfReader = 3;
  
  @Parameter(names = "--validator-max-run-time", description = "")
  private long validatorMaxRuntime = -1;
  
  @Parameter(names = "--validator-message-wait-timeout", description = "")
  private long validatorMessageWaitTimeout = 30000;
  
  @Override
  public void execute(Shell s, CommandInput cmdInput) throws Exception {
    ScribenginShell shell = (ScribenginShell) s;
    VMClient vmClient = shell.getVMClient();
    TrackingDataflowBuilder dflBuilder = setup(vmClient);
    vmClient.uploadApp(localAppHome, dfsAppHome);
    execute(shell, dflBuilder);
  }
  
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient = shell.getVMClient();
    submitVMGenerator(vmClient, dflBuilder);
    submitDataflow(shell, dflBuilder.buildDataflow());
    submitVMValidator(vmClient, dflBuilder);
  }

  protected void submitVMGenerator(VMClient vmClient, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMConfig vmGeneratorConfig = dflBuilder.buildVMTMGeneratorKafka();
    new VMSubmitter(vmClient, dfsAppHome, vmGeneratorConfig).submit().waitForRunning(30000);
  }
  
  protected DataflowSubmitter submitDataflow(ScribenginShell shell, Dataflow<TrackingMessage, TrackingMessage> dfl) throws Exception {
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));
    DataflowSubmitter submitter = new DataflowSubmitter(shell.getScribenginClient(), dfl);
    submitter.submit().waitForDataflowRunning(60000);
    return submitter;
  }
  
  protected void submitVMValidator(VMClient vmClient, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMConfig vmValidatorConfig = null;
    if("hdfs".equalsIgnoreCase(dataflowStorage)) {
      vmValidatorConfig = dflBuilder.buildHDFSVMTMValidator();
    } else {
      vmValidatorConfig = dflBuilder.buildKafkaVMTMValidator();
    }
    new VMSubmitter(vmClient, dfsAppHome, vmValidatorConfig).submit().waitForRunning(30000);
  }
  
  protected TrackingDataflowBuilder setup(VMClient vmClient) {
    TrackingDataflowBuilder dflBuilder = new TrackingDataflowBuilder(dataflowId);
    dflBuilder.
      setNumOfWorker(numOfWorker).
      setNumOfExecutorPerWorker(numOfExecutorPerWorker).
      setTrackingWindowSize(trackingWindowSize).
      setSlidingWindowSize(slidingWindowSize).
      setDefaultParallelism(dataflowDefaultParallelism).
      setDefaultReplication(dataflowDefaultReplication);
    
    if("hdfs".equalsIgnoreCase(dataflowStorage)) {
      dflBuilder.setHDFSAggregateOutput();
      dflBuilder.getTrackingConfig().setHDFSStorageDir("/storage/hdfs/tracking-aggregate");
    }
    
    dflBuilder.getTrackingConfig().setGeneratorNumOfWriter(generatorNumOfWriter);
    dflBuilder.getTrackingConfig().setGeneratorBreakInPeriod(generatorBreakInPeriod);
    dflBuilder.getTrackingConfig().setNumOfChunk(generatorNumOfChunks);
    dflBuilder.getTrackingConfig().setNumOfMessagePerChunk(generatorNumOfMessagePerChunk);
    dflBuilder.getTrackingConfig().setKafkaZKConnects(vmClient.getRegistry().getRegistryConfig().getConnect());
    dflBuilder.getTrackingConfig().setKafkaNumOfPartition(generatorKafkaNumOfPartition);
    dflBuilder.getTrackingConfig().setKafkaNumOfReplication(generatorKafkaNumOfReplication);
    dflBuilder.getTrackingConfig().setKafkaMessageWaitTimeout(validatorMessageWaitTimeout);;
    
    if(validatorMaxRuntime < 1) {
      validatorMaxRuntime = 180000 + (generatorNumOfMessagePerChunk * generatorNumOfChunks)/3;
    }
    
    dflBuilder.getTrackingConfig().setValidatorMaxRuntime(validatorMaxRuntime);
    dflBuilder.getTrackingConfig().setValidatorNumOfReader(validatorNumOfReader);
    return dflBuilder;
  }
  
  @Override
  public String getDescription() { return "Tracking app launcher"; }
}
