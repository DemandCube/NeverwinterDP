package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.shell.Executor;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMSubmitter;

public class VMLogValidatorExecutor extends Executor {
  private LogSampleConfig config;
  
  public VMLogValidatorExecutor(ScribenginShell shell, LogSampleConfig config) {
    super(shell);
    this.config = config;
  }
  
  public void onInit() {
  }
  
  @Override
  public void run() {
    long start = System.currentTimeMillis() ;
    System.out.println("Submit The Validator App");
    try {
      VMConfig vmConfig = new VMConfig() ;
      vmConfig.setRegistryConfig(config.registryConfig);
      vmConfig.setName("log-validator");
      vmConfig.addRoles("log-validator");
      vmConfig.addProperty("num-of-message-per-partition", config.logGeneratorNumOfMessagePerExecutor);
      vmConfig.addProperty("num-of-executor", config.logValidatorNumOfExecutorPerVM);
      vmConfig.addProperty("wait-for-message-timeout", config.logValidatorWaitForMessageTimeout);
      vmConfig.addProperty("wait-for-termination", config.logValidatorWaitForTermination);
      vmConfig.addProperty("validate-topic", config.logValidatorValidateTopic);
      vmConfig.setVmApplication(VMLogMessageValidatorApp.class.getName());
      VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), config.dfsAppHome, vmConfig);
      vmSubmitter.submit();
      vmSubmitter.waitForRunning(30000);
      vmSubmitter.waitForTerminated(config.logValidatorWaitForTermination);
      System.out.println("Finish The Validator App");
      System.out.println("Execute Time: " + (System.currentTimeMillis() - start) + "ms");
      LogSampleRegistry appRegistry = new LogSampleRegistry(shell.getVMClient().getRegistry(), false);
      System.out.println(LogMessageReport.getFormattedReport("Generated Report", appRegistry.getGeneratedReports()));
      System.out.println(LogMessageReport.getFormattedReport("Validate Report", appRegistry.getValidateReports()));
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
