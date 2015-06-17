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
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.VMSubmitter;

public class LogSampleRunner {
  LogSampleConfig config ;
  ScribenginShell shell ;
  
  public LogSampleRunner(String[] args) throws Exception {
    config = new LogSampleConfig() ;
    new JCommander(config, args);
    Registry registry = config.registryConfig.newInstance();
    registry.connect();
    VMClient vmClient = new VMClient(registry);
    shell = new ScribenginShell(vmClient);
  }
  
  public void submitVMLogGeneratorApp() throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.setRegistryConfig(config.registryConfig);
    vmConfig.setName("log-generator");
    vmConfig.addRoles("log-generator");
    vmConfig.setVmApplication(VMLogMessageGeneratorApp.class.getName());
    vmConfig.addProperty("num-of-message", config.logGeneratorNumOfMessage);
    vmConfig.addProperty("message-size", config.logGeneratorMessageSize);
    VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), config.appHome, vmConfig);
    vmSubmitter.submit();
    vmSubmitter.waitForRunning(30000);
  }
  
  public VMSubmitter submitVMLogValidatorApp() throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.setRegistryConfig(config.registryConfig);
    vmConfig.setName("log-validator");
    vmConfig.addRoles("log-validator");
    vmConfig.setVmApplication(VMLogMessageValidatorApp.class.getName());
    VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), config.appHome, vmConfig);
    vmSubmitter.submit();
    vmSubmitter.waitForRunning(30000);
    return vmSubmitter ;
  }
  
  public void submitLogSampleDataflowChain() throws Exception {
    String json = IOUtil.getFileContentAsString("src/app/conf/local/log-dataflow-chain.json") ;
    DataflowChainConfig dflChainconfig = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
    OrderDataflowChainSubmitter submitter = 
        new OrderDataflowChainSubmitter(shell.getScribenginClient(), null, dflChainconfig);
    if(config.debugDataflowTask) {
      submitter.enableDataflowTaskDebugger();
    }
    submitter.submit(45000);
    submitter.waitForTerminated(45000);
//    shell.execute("dataflow info --dataflow-id log-splitter-dataflow-1") ;
//    shell.execute("dataflow info --dataflow-id log-persister-dataflow-info") ;
//    shell.execute("dataflow info --dataflow-id log-persister-dataflow-warn") ;
//    shell.execute("dataflow info --dataflow-id log-persister-dataflow-error") ;
  }
  
  static public void main(String[] args) throws Exception {
    LogSampleRunner runner = new LogSampleRunner(args);
    runner.submitVMLogGeneratorApp();
    Thread.sleep(35000);
    runner.submitLogSampleDataflowChain();
    runner.submitVMLogValidatorApp().waitForTerminated(15000);;
  }
}
