package com.neverwinterdp.os;

import java.lang.management.MemoryUsage;
import java.util.Date;

import com.neverwinterdp.util.text.ByteUtil;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class MemoryInfo {
  private String name;
  private Date   timestamp ;
  private long   init;
  private long   used;
  private long   committed;
  private long   max;
  
  public MemoryInfo() {
  }
  
  public MemoryInfo(String name, MemoryUsage mUsage) {
    this.name = name;
    this.timestamp = new Date(System.currentTimeMillis());
    init = mUsage.getInit() ;
    max = mUsage.getMax() ;
    used = mUsage.getUsed() ;
    committed = mUsage.getCommitted() ;
  }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public Date getTimestamp() { return this.timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp  = timestamp;}
  
  public long getInit() { return init; }
  public void setInit(long init) { this.init = init; }

  public long getUsed() { return used ; }
  public void setUsed(long used) { this.used = used; }

  public long getCommitted() { return committed ;}
  public void setCommitted(long committed) { this.committed = committed; }

  public long getMax() { return max; }
  public void setMax(long max) { this.max = max; }
  
  public String toString() { return getFormattedText(this) ; }
  
  static public String getFormattedText(MemoryInfo ... memoryInfo) {
    String[] header = { "Timestamp", "Name", "Init", "Max", "Used", "Committed"} ;
    TabularFormater formater = new TabularFormater(header);
    for(MemoryInfo sel : memoryInfo) {
      formater.addRow(
          sel.getName(),
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          ByteUtil.byteToHumanReadable(sel.getInit()),
          ByteUtil.byteToHumanReadable(sel.getMax()),
          ByteUtil.byteToHumanReadable(sel.getUsed()),
          ByteUtil.byteToHumanReadable(sel.getCommitted())
      ); 
    }
    return formater.getFormattedText();
  }
}