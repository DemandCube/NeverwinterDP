package com.neverwinterdp.hqueue;

public class HQueueCursorRead {
  private String name ;
  
  public HQueueCursorRead() {
  }
  
  public HQueueCursorRead(String name) {
    this.name = name ;
  }

  public String getName() { return name; }

  public void setName(String name) { this.name = name; }
}
