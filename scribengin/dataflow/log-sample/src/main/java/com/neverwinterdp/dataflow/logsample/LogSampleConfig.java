package com.neverwinterdp.dataflow.logsample;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.RegistryConfig;

public class LogSampleConfig {
  @Parameter(names = "--app-home", description = "Local App Home")
  String appHome ;
  
  @Parameter(names = "--log-generator-message-size", description = "Log generator message size")
  int logGeneratorMessageSize ;
  
  @Parameter(names = "--log-generator-num-of-message", description = "Log generator message size")
  int logGeneratorNumOfMessage = 100;
  
  @ParametersDelegate
  RegistryConfig registryConfig   = new RegistryConfig();

  @Parameter(names = "--debug-dataflow-task", description = "Debug the dataflow task!")
  boolean debugDataflowTask = false;
}