package com.neverwinterdp.dataflow.logsample.chain;

import com.neverwinterdp.dataflow.logsample.MessageReport;
import com.neverwinterdp.dataflow.logsample.MessageReportRegistry;
import com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.shell.Executor;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMSubmitter;

public class VMLogValidatorExecutor extends Executor {
  private LogSampleChainConfig config;
  
  public VMLogValidatorExecutor(ScribenginShell shell, LogSampleChainConfig config) {
    super(shell);
    this.config = config;
  }
  
  public void onInit() {
  }
  
  @Override
  public void run() {
    long start = System.currentTimeMillis() ;
    System.out.println("Submit The Validator App");
    String reportPath = "/applications/log-sample/message-report";
    try {
      VMConfig vmConfig = new VMConfig() ;
      vmConfig.setRegistryConfig(config.registryConfig);
      vmConfig.setName("log-validator");
      vmConfig.addRoles("log-validator");
      vmConfig.addProperty("num-of-message-per-partition", config.logGeneratorNumOfMessage);
      vmConfig.addProperty("wait-for-termination", config.logValidatorWaitForTermination);
      vmConfig.addProperty("report-path", reportPath);
      if(config.logValidatorValidateKafka != null) {
        vmConfig.addProperty("validate-kafka", config.logValidatorValidateKafka);
      }
      if(config.logValidatorValidateHdfs != null) {
        vmConfig.addProperty("validate-hdfs", config.logValidatorValidateHdfs);
      }
      if(config.logValidatorValidateS3 != null) {
        vmConfig.addProperty("validate-s3", config.logValidatorValidateS3);
      }
      vmConfig.setVmApplication(VMLogMessageValidatorApp.class.getName());
      VMSubmitter vmSubmitter = new VMSubmitter(shell.getVMClient(), config.dfsAppHome, vmConfig);
      vmSubmitter.submit();
      vmSubmitter.waitForRunning(30000);
      vmSubmitter.waitForTerminated(config.logValidatorWaitForTermination);
      System.out.println("Finish The Validator App");
      System.out.println("Execute Time: " + (System.currentTimeMillis() - start) + "ms");
      MessageReportRegistry appRegistry = 
        new MessageReportRegistry(shell.getVMClient().getRegistry(), reportPath, false);
      System.out.println(MessageReport.getFormattedReport("Generated Report", appRegistry.getGeneratedReports()));
      System.out.println(MessageReport.getFormattedReport("Validate Report", appRegistry.getValidateReports()));
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
