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
  private String localAppHome        = null;
  
  @Parameter(names = "--dfs-app-home", description = "Generator num of chunk")
  private String dfsAppHome        = "/applications/tracking-sample";
  
  @Parameter(names = "--tracking-report-path", description = "Generator num of chunk")
  private String trackingReportPath  = "/applications/tracking-sample/reports";

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
  
  @Parameter(names = "--validator-num-of-reader", description = "")
  private int validatorNumOfReader = 3;
  
  @Parameter(names = "--validator-max-run-time", description = "")
  private long validatorMaxRuntime = -1;
  
  @Override
  public void execute(Shell s, CommandInput cmdInput) throws Exception {
    ScribenginShell shell = (ScribenginShell) s;
    VMClient vmClient = shell.getVMClient();
    
    vmClient.uploadApp(localAppHome, dfsAppHome);
    
    TrackingDataflowBuilder dflBuilder = new TrackingDataflowBuilder(dataflowId);
    dflBuilder.
      setNumOfWorker(numOfWorker).
      setNumOfExecutorPerWorker(numOfExecutorPerWorker).
      setTrackingWindowSize(10000);
    
    if("hdfs".equalsIgnoreCase(dataflowStorage)) {
      dflBuilder.setHDFSAggregateOutput();
      dflBuilder.getTrackingConfig().setHDFSAggregateLocation("/storage/hdfs/tracking-aggregate");
    }
    
    dflBuilder.getTrackingConfig().setReportPath(trackingReportPath);
    
    dflBuilder.getTrackingConfig().setGeneratorNumOfWriter(generatorNumOfWriter);
    dflBuilder.getTrackingConfig().setGeneratorBreakInPeriod(generatorBreakInPeriod);
    dflBuilder.getTrackingConfig().setNumOfChunk(generatorNumOfChunks);
    dflBuilder.getTrackingConfig().setNumOfMessagePerChunk(generatorNumOfMessagePerChunk);
    dflBuilder.getTrackingConfig().setKafkaZKConnects(vmClient.getRegistry().getRegistryConfig().getConnect());
    dflBuilder.getTrackingConfig().setKafkaNumOfPartition(generatorKafkaNumOfPartition);
    dflBuilder.getTrackingConfig().setKafkaNumOfReplication(generatorKafkaNumOfReplication);
    
    if(validatorMaxRuntime < 1) {
      validatorMaxRuntime = 180000 + (generatorNumOfMessagePerChunk * generatorNumOfChunks)/3;
    }
    
    dflBuilder.getTrackingConfig().setValidatorMaxRuntime(validatorMaxRuntime);
    dflBuilder.getTrackingConfig().setValidatorNumOfReader(validatorNumOfReader);
    
    VMConfig vmGeneratorConfig = dflBuilder.buildVMTMGeneratorKafka();
    new VMSubmitter(vmClient, dfsAppHome, vmGeneratorConfig).submit().waitForRunning(30000);
    
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));
    
    new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForRunning(60000);
    
    VMConfig vmValidatorConfig = dflBuilder.buildKafkaVMTMValidator();
    new VMSubmitter(vmClient, dfsAppHome, vmValidatorConfig).submit().waitForRunning(30000);
  }

  @Override
  public String getDescription() { return "Tracking app launcher"; }
}
