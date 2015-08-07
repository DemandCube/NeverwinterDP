package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Console;
import com.neverwinterdp.vm.client.shell.Shell;

public class ScribenginShell extends Shell {
  private ScribenginClient scribenginClient;

  public ScribenginShell(Registry registry) {
    this(registry, new Console());
  }

  public ScribenginShell(Registry registry, Console console) {
    this(new VMClient(registry), console);
  }

  public ScribenginShell(ScribenginClient scribenginClient, Console console) {
    super(scribenginClient.getVMClient(), console);
    this.scribenginClient = scribenginClient;
    add("scribengin", new ScribenginCommand());
    add("dataflow", new DataflowCommand());
    add("dataflow-test", new DataflowTestCommand());
  }
  
  public ScribenginShell(ScribenginClient scribenginClient) {
    this(scribenginClient.getVMClient(), new Console());
  }
  
  public ScribenginShell(VMClient vmClient, Console console) {
    this(new ScribenginClient(vmClient), console);
  }
  
  public ScribenginShell(VMClient vmClient) {
    this(vmClient, new Console());
  }

  public ScribenginClient getScribenginClient() { return this.scribenginClient; }
}
