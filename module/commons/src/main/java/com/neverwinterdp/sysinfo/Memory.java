package com.neverwinterdp.sysinfo;

import java.io.Serializable;
import java.lang.management.MemoryUsage;

@SuppressWarnings("serial")
public class Memory implements Serializable {
  private String name;
  private long   init;
  private long   committed;
  private long   max;
  private long   used;
    
  public Memory() {
  }
  
  public Memory(String name, MemoryUsage mUsage) {
    this.name = name;
    init      = mUsage.getInit() ;
    max       = mUsage.getMax() ;
    used      = mUsage.getUsed() ;
    committed = mUsage.getCommitted();
  }

  public String getName() { return name; }
  public void   setName(String name) { this.name = name; }

  public long getInit() { return init; }
  public void setInit(long init) { this.init = init; }
  
  public long getCommitted() { return committed ;}
  public void setCommitted(long committed) { this.committed = committed; }

  public long getMax() { return max; }
  public void setMax(long max) { this.max = max; }
  
  public long getUsed() { return used ; }
  public void setUsed(long used) { this.used = used; }
}