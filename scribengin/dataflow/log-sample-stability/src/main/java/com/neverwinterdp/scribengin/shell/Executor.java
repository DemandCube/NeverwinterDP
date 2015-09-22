package com.neverwinterdp.scribengin.shell;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

abstract public class Executor implements Runnable {
  
  protected ScribenginShell shell;
  
  
  public Executor(ScribenginShell shell) {
    this.shell = shell;
  }
  
}
