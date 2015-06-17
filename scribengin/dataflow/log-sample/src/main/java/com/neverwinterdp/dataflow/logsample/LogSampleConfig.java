package com.neverwinterdp.dataflow.logsample;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.RegistryConfig;

public class LogSampleConfig {
  @Parameter(names = "--app-home", description = "Local App Home")
  String appHome ;

  @Parameter(names = "--log-generator-num-of-vm", description = "Log generator message size")
  int logGeneratorNumOfVM = 1;
  
  @Parameter(names = "--log-generator-num-of-executor-per-vm", description = "Log generator message size")
  int logGeneratorNumOfExecutorPerVm = 1;
  
  @Parameter(names = "--log-generator-num-of-message-per-executor", description = "Log generator message size")
  int logGeneratorNumOfMessagePerExecutor = 1000;
  
  @Parameter(names = "--log-generator-message-size", description = "Log generator message size")
  int logGeneratorMessageSize ;
  
  @Parameter(names = "--log-validator-num-of-vm", description = "Log generator message size")
  int logValidatorNumOfVM = 1;
  
  @Parameter(names = "--log-validator-num-of-executor-per-vm", description = "Log generator message size")
  int logValidatorNumOfExecutorPerVM = 3;
  
  @Parameter(names = "--log-validator-wait-for-message-timeout", description = "Log generator message size")
  long logValidatorWaitForMessageTimeout = 5000;
  
  @Parameter(names = "--log-validator-wait-for-termination", description = "Log generator message size")
  long logValidatorWaitForTermination = 60000;
  
  @Parameter(names = "--log-validator-validate-topic", description = "Topic to validate")
  String logValidatorValidateTopic = "log4j.aggregate";
  
  @ParametersDelegate
  RegistryConfig registryConfig   = new RegistryConfig();

  @Parameter(names = "--debug-dataflow-task", description = "Debug the dataflow task!")
  boolean debugDataflowTask = false;
}