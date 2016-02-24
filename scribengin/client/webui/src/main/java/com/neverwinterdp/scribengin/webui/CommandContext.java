package com.neverwinterdp.scribengin.webui;

import com.neverwinterdp.scribengin.ScribenginClient;

public class CommandContext {
  private ScribenginClient scribenginClient;
  
  public CommandContext(ScribenginClient client) {
    scribenginClient = client;
  }

  public ScribenginClient getScribenginClient() { return scribenginClient; }
}
