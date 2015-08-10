package com.neverwinterdp.dataflow.logsample.chain;

import com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.shell.ShellCommandExecutor;

public class VMLogGeneratorExecutor extends ShellCommandExecutor {

  public VMLogGeneratorExecutor(ScribenginShell shell, int id, LogSampleChainConfig config) {
    super(shell);
    String cmdLine = 
      "vm submit " +
      "  --dfs-app-home " + config.dfsAppHome +
      "  --registry-connect " + config.registryConfig.getConnect() +
      "  --registry-db-domain " + config.registryConfig.getDbDomain() +
      "  --registry-implementation " + config.registryConfig.getRegistryImplementation() +
      "  --name vm-log-generator-" + id + " --role vm-log-generator" + 
      "  --vm-application " + VMToKafkaLogMessageGeneratorApp.class.getName() + 
      "  --prop:num-of-message=" + config.logGeneratorNumOfMessage +
      "  --prop:message-size=" + config.logGeneratorMessageSize +
      "  --prop:report-path=" + config.reportPath ;
    setCmdLine(cmdLine);
  }

}
