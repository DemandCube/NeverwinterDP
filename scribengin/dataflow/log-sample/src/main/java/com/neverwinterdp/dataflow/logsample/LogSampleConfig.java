package com.neverwinterdp.dataflow.logsample;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.RegistryConfig;

public class LogSampleConfig {
  @Parameter(names = "--upload-app", description = "Local App Home")
  String uploadApp ;

  @Parameter(names = "--dfs-app-home", description = "Local App Home")
  String dfsAppHome ;

  @Parameter(names = "--log-generator-num-of-vm", description = "Log generator message size")
  public int logGeneratorNumOfVM = 1;
  
  @Parameter(names = "--log-generator-num-of-executor-per-vm", description = "Log generator message size")
  public int logGeneratorNumOfExecutorPerVm = 1;
  
  @Parameter(names = "--log-generator-num-of-message-per-executor", description = "Log generator message size")
  public int logGeneratorNumOfMessagePerExecutor = 1000;
  
  @Parameter(names = "--log-generator-message-size", description = "Log generator message size")
  public int logGeneratorMessageSize ;
  
  @Parameter(names = "--log-generator-wait-before-exit", description = "Need to wait to make sure the log component forward the data to kafka, es")
  public long logGeneratorWaitBeforeExit = 45000 ;
  
  @Parameter(names = "--log-validator-num-of-vm", description = "Log generator message size")
  public int logValidatorNumOfVM = 1;
  
  @Parameter(names = "--log-validator-num-of-executor-per-vm", description = "Log generator message size")
  public int logValidatorNumOfExecutorPerVM = 3;
  
  @Parameter(names = "--log-validator-wait-for-message-timeout", description = "Log generator message size")
  public long logValidatorWaitForMessageTimeout = 5000;
  
  @Parameter(names = "--log-validator-wait-for-termination", description = "Log generator message size")
  public long logValidatorWaitForTermination = 60000;
  
  @Parameter(names = "--log-validator-validate-topic", description = "Topic to validate")
  public String logValidatorValidateTopic = "log4j.aggregate";
  
  @ParametersDelegate
  public RegistryConfig registryConfig   = new RegistryConfig();

  @Parameter(names = "--dataflow-descriptor", description = "Debug the dataflow task!")
  public String dataflowDescriptor = "src/app/conf/local/log-dataflow-chain.json";
 
  @Parameter(names = "--dataflow-wait-for-submit-timeout", description = "Debug the dataflow task!")
  public long dataflowWaitForSubmitTimeout = 60000;
 
  @Parameter(names = "--dataflow-wait-for-termination-timeout", description = "Debug the dataflow task!")
  public long dataflowWaitForTerminationTimeout = 90000;
 
  @Parameter(names = "--dataflow-task-debug", description = "Debug the dataflow task!")
  public boolean dataflowTaskDebug = false;
  
  @Parameter(names = "--dataflow-failure-simulation-worker", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationWorker = false;
  
  @Parameter(names = "--dataflow-failure-simulation-master", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationMaster = false;
  
  @Parameter(names = "--dataflow-failure-simulation-start-stop-resume", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationStartStopResume = false;
  
  @Parameter(names = "--dataflow-failure-simulation-wait-before-start", description = "Enable the dataflow worker failure simulation!")
  public long dataflowFailureSimulationWaitBeforeStart = 15000;
}