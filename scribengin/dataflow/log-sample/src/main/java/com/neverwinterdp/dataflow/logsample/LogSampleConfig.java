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
  
  @Parameter(names = "--log-generator-wait-for-ready", description = "Log generator message size")
  public long logGeneratorWaitForReady = 15000 ;
  
  @Parameter(names = "--log-generator-num-of-message", description = "Log generator message size")
  public int logGeneratorNumOfMessage = 1000;
  
  @Parameter(names = "--log-generator-message-size", description = "Log generator message size")
  public int logGeneratorMessageSize ;
  
  @Parameter(names = "--log-validator-num-of-vm", description = "Log generator message size")
  public int logValidatorNumOfVM = 1;
  
  @Parameter(names = "--log-validator-wait-for-termination", description = "Log generator message size")
  public long logValidatorWaitForTermination = 60000;
  
  @Parameter(names = "--log-validator-validate-kafka", description = "Topic to validate")
  public String logValidatorValidateKafka = null;
  
  @Parameter(names = "--log-validator-validate-hdfs", description = "Topic to validate")
  public String logValidatorValidateHdfs = null;
  
  @Parameter(names = "--log-validator-validate-s3", description = "Topic to validate")
  public String logValidatorValidateS3 = null;
  
  @ParametersDelegate
  public RegistryConfig registryConfig   = new RegistryConfig();

  @Parameter(names = "--dataflow-descriptor", description = "Debug the dataflow task!")
  public String dataflowDescriptor = "src/app/conf/local/log-dataflow-chain.json";
 
  @Parameter(names = "--dataflow-wait-for-submit-timeout", description = "Debug the dataflow task!")
  public long dataflowWaitForSubmitTimeout = 60000;
 
  @Parameter(names = "--dataflow-wait-for-termination-timeout", description = "Debug the dataflow task!")
  public long dataflowWaitForTerminationTimeout = 90000;
 
  @Parameter(names = "--dataflow-task-executor-dedicated", description = "")
  public boolean dataflowTaskExecutorDedicated = false;
  
  
  @Parameter(names = "--dataflow-task-debug", description = "Debug the dataflow task!")
  public boolean dataflowTaskDebug = false;
  
  @Parameter(names = "--dataflow-failure-simulation-worker", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationWorker = false;
  
  @Parameter(names = "--dataflow-failure-simulation-master", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationMaster = false;
  
  @Parameter(names = "--dataflow-failure-simulation-start-stop-resume", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationStartStopResume = false;
  
  @Parameter(names = "--dataflow-failure-simulation-simulate-kill", description = "Enable the dataflow worker failure simulation!")
  public boolean dataflowFailureSimulationSimulateKill = false;
  
  @Parameter(names = "--dataflow-failure-simulation-wait-before-start", description = "Enable the dataflow worker failure simulation!")
  public long dataflowFailureSimulationWaitBeforeStart = 15000;
  
  @Parameter(names = "--dataflow-failure-simulation-max-kill", description = "Enable the dataflow worker failure simulation!")
  public int dataflowFailureSimulationMaxKill = 25;
  
  @Parameter(names = "--dataflow-failure-simulation-period", description = "Enable the dataflow worker failure simulation!")
  public int dataflowFailureSimulationPeriod = 15000;
}