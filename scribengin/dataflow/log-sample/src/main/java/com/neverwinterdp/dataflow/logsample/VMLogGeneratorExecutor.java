package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.dataflow.logsample.vm.VMLogMessageGeneratorApp;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.shell.ShellCommandExecutor;

public class VMLogGeneratorExecutor extends ShellCommandExecutor {

  public VMLogGeneratorExecutor(ScribenginShell shell, int id, LogSampleConfig config) {
    super(shell);
    String cmdLine = 
        "vm submit " +
        "  --dfs-app-home " + config.dfsAppHome +
        "  --registry-connect " + config.registryConfig.getConnect() +
        "  --registry-db-domain " + config.registryConfig.getDbDomain() +
        "  --registry-implementation " + config.registryConfig.getRegistryImplementation() +
        "  --name vm-log-generator-" + id + " --role vm-log-generator" + 
        "  --vm-application " + VMLogMessageGeneratorApp.class.getName() + 
        "  --prop:num-of-executor=" + config.logGeneratorNumOfExecutorPerVm +
        "  --prop:num-of-message-per-executor=" + config.logGeneratorNumOfMessagePerExecutor +
        "  --prop:message-size=" + config.logGeneratorMessageSize ;
    setCmdLine(cmdLine);
  }

}
