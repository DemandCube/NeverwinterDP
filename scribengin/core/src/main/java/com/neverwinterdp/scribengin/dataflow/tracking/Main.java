package com.neverwinterdp.scribengin.dataflow.tracking;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.VMSubmitter;

public class Main {
  @ParametersDelegate
  private RegistryConfig registryConfig;

  @Parameter(names = "--local-app-home", required = true, description = "Generator num of chunk")
  private String localAppHome        = null;
  
  @Parameter(names = "--dfs-app-home", description = "Generator num of chunk")
  private String dfsAppHome        = "/applications/tracking-sample";
  
  @Parameter(names = "--tracking-report-path", description = "Generator num of chunk")
  private String trackingReportPath  = "/applications/tracking-sample/reports";

  @Parameter(names = "--generator-num-of-chunk", description = "Generator num of chunk")
  private int generatorNumOfChunks          = 10;
  
  @Parameter(names = "--generator-num-of-message-per-chunk", description = "Generator num of message per chunk")
  private int generatorNumOfMessagePerChunk = 1000;
  
  @Parameter(names = "--generator-num-of-writer", description = "Generator num of chunk")
  private int generatorNumOfWriter = 3;
  
  @Parameter(names = "--generator-kafka-num-of-partition", description = "")
  private int generatorKafkaNumOfPartition = 8;
  
  @Parameter(names = "--generator-kafka-num-of-replication", description = "")
  private int generatorKafkaNumOfReplication = 2;
  
  @Parameter(names = "--dataflow-id", description = "Generator num of chunk")
  private String dataflowId        = "tracking";
  
  @Parameter(names = "--dataflow-num-of-worker", description = "Dataflow num of worker")
  private int numOfWorker = 8;
  
  @Parameter(names = "--dataflow-num-of-executor-per-worker", description = "Dataflow num of worker")
  private int numOfExecutorPerWorker = 2;
  
  @Parameter(names = "--validator-num-of-reader", description = "")
  private int validatorNumOfReader = 3;
  
  @Parameter(names = "--validator-max-run-time", description = "")
  private int validatorMaxRuntime = 90000;
  
  public Main(String[] args) throws Exception {
    new JCommander(this, args);
  }
  
  public void run() throws Exception {
    Registry registry = registryConfig.newInstance();
    ScribenginShell shell = new ScribenginShell(registry);
    VMClient vmClient = shell.getVMClient();
    
    vmClient.uploadApp(localAppHome, dfsAppHome);
    
    TrackingDataflowBuilder dflBuilder = new TrackingDataflowBuilder(dataflowId);
    dflBuilder.
      setNumOfWorker(numOfWorker).
      setNumOfExecutorPerWorker(numOfExecutorPerWorker);
    
    dflBuilder.getTrackingConfig().setReportPath(trackingReportPath);
    
    dflBuilder.getTrackingConfig().setGeneratorNumOfWriter(generatorNumOfWriter);
    dflBuilder.getTrackingConfig().setNumOfChunk(generatorNumOfChunks);
    dflBuilder.getTrackingConfig().setNumOfMessagePerChunk(generatorNumOfMessagePerChunk);
    dflBuilder.getTrackingConfig().setKafkaZKConnects(registryConfig.getConnect());
    dflBuilder.getTrackingConfig().setKafkaNumOfPartition(generatorKafkaNumOfPartition);
    dflBuilder.getTrackingConfig().setKafkaNumOfReplication(generatorKafkaNumOfReplication);
    
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
  
  static public void main(String[] args) throws Exception {
    Main main = new Main(args);
    main.run();
  }
}
