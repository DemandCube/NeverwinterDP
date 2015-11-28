package com.neverwinterdp.nstoragebak;

public class NStorageCursorRead {
  private String name ;
  
  public NStorageCursorRead() {
  }
  
  public NStorageCursorRead(String name) {
    this.name = name ;
  }

  public String getName() { return name; }

  public void setName(String name) { this.name = name; }
}
