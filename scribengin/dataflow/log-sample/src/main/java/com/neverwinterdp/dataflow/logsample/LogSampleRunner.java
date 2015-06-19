package com.neverwinterdp.dataflow.logsample;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.dataflow.logsample.vm.VMLogMessageGeneratorApp;
import com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.chain.DataflowChainConfig;
import com.neverwinterdp.scribengin.dataflow.chain.OrderDataflowChainSubmitter;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.GroupVMSubmitter;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.VMSubmitter;
import com.neverwinterdp.vm.client.YarnVMClient;

public class LogSampleRunner {
  LogSampleConfig config ;
  ScribenginShell shell ;
  
  public LogSampleRunner(String[] args) throws Exception {
    config = new LogSampleConfig() ;
    new JCommander(config, args);
    Registry registry = config.registryConfig.newInstance();
    registry.connect();
    
    VMClient vmClient = null;
    if(config.dfsAppHome != null) {
      String hadoopMaster = System.getProperty("shell.hadoop-master");
      HadoopProperties hadoopProps = new HadoopProperties() ;
      hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
      hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
      vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    } else {
      vmClient = new VMClient(registry);
    }
    shell = new ScribenginShell(vmClient);
  }
  
  public void uploadApp() throws Exception {
    if(config.uploadApp != null) {
      shell.getVMClient().uploadApp(config.uploadApp, config.dfsAppHome);
    }
  }
  
  public void submitVMLogGeneratorApp() throws Exception {
    GroupVMSubmitter groupVMSubmitter = new GroupVMSubmitter(shell.getVMClient());
    for(int i = 0; i < config.logGeneratorNumOfVM; i++) {
      VMConfig vmConfig = new VMConfig();
      vmConfig.setRegistryConfig(config.registryConfig);
      vmConfig.setName("vm-log-generator-" + (i + 1));
      vmConfig.addRoles("vm-log-generator");
      vmConfig.setVmApplication(VMLogMessageGeneratorApp.class.getName());
      vmConfig.addProperty("num-of-executor", config.logGeneratorNumOfExecutorPerVm);
      vmConfig.addProperty("num-of-message-per-executor", config.logGeneratorNumOfMessagePerExecutor);
      vmConfig.addProperty("message-size", config.logGeneratorMessageSize);
      groupVMSubmitter.add(config.dfsAppHome, vmConfig);
    }
    groupVMSubmitter.submitAndWaitForRunning(45000);
  }
  
  public VMSubmitter submitVMLogValidatorApp() throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.setRegistryConfig(config.registryConfig);
    vmConfig.setName("log-validator");
    vmConfig.addRoles("log-validator");
    vmConfig.addProperty("num-of-executor", config.logValidatorNumOfExecutorPerVM);
    vmConfig.addProperty("wait-for-message-timeout", config.logValidatorWaitForMessageTimeout);
    vmConfig.addProperty("wait-for-termination", config.logValidatorWaitForTermination);
    vmConfig.addProperty("validate-topic", config.logValidatorValidateTopic);
    vmConfig.setVmApplication(VMLogMessageValidatorApp.class.getName());
    VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), config.dfsAppHome, vmConfig);
    vmSubmitter.submit();
    vmSubmitter.waitForRunning(30000);
    vmSubmitter.waitForTerminated(config.logValidatorWaitForTermination);
    return vmSubmitter ;
  }
  
  public void submitLogSampleDataflowChain() throws Exception {
    String json = IOUtil.getFileContentAsString(config.dataflowDescriptor) ;
    DataflowChainConfig dflChainconfig = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
    OrderDataflowChainSubmitter submitter = 
        new OrderDataflowChainSubmitter(shell.getScribenginClient(), config.dfsAppHome, dflChainconfig);
    if(config.dataflowTaskDebug) {
      submitter.enableDataflowTaskDebugger();
    }
    submitter.submit(config.dataflowWaitForSubmitTimeout);
    submitter.waitForTerminated(config.dataflowWaitForTerminationTimeout);
  }
  
  static public void main(String[] args) throws Exception {
    LogSampleRunner runner = new LogSampleRunner(args);
    runner.uploadApp();
    runner.submitVMLogGeneratorApp();
    Thread.sleep(15000);
    runner.submitLogSampleDataflowChain();
    runner.submitVMLogValidatorApp();
  }
}
