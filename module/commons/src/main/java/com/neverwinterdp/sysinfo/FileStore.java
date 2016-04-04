package com.neverwinterdp.sysinfo;

import java.io.IOException;
import java.io.Serializable;

@SuppressWarnings("serial")
public class FileStore implements Serializable {
  private String name ;
  private String type;
  private long   total ;
  private long   used  ;
  private long   available;

  public FileStore() {} 
  
  public FileStore(java.nio.file.FileStore store) throws IOException {
    name      = store.name();
    total     = store.getTotalSpace();
    used      =  store.getTotalSpace() - store.getUnallocatedSpace();
    available = store.getUsableSpace();
  } 
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getType() { return type; }
  public void setType(String type) { this.type = type; }

  public long getTotal() { return total; }
  public void setTotal(long total) { this.total = total; }
  
  public long getUsed() { return used; }
  public void setUsed(long used) { this.used = used; }
  
  public long getAvailable() { return available; }
  public void setAvailable(long available) { this.available = available; }
}
