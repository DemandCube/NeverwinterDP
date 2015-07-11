package com.neverwinterdp.scribengin.shell;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

abstract public class ShellCommandExecutor extends Executor {
  private   String          cmdLine ;
  
  public ShellCommandExecutor(ScribenginShell shell) {
    super(shell);
  }
  
  public ShellCommandExecutor(ScribenginShell shell, String cmdLine) {
    super(shell);
    this.cmdLine = cmdLine ;
  }

  public void setCmdLine(String cmdLine) { this.cmdLine = cmdLine; }

  @Override
  public void run() {
    try {
      shell.execute(cmdLine);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
