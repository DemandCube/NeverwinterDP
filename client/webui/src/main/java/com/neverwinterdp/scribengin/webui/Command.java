package com.neverwinterdp.scribengin.webui;

abstract public class Command {
  
  abstract public <T> T execute(CommandContext ctx) throws Exception ;

  protected void debug(String mesg) {
    System.out.println("[" + getClass().getSimpleName() + "]: " + mesg);
  }
}
