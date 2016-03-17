package com.neverwinterdp.os;

import java.io.Serializable;
import java.lang.management.MemoryUsage;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.ByteUtil;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

@SuppressWarnings("serial")
public class MemoryInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp ;
  private String host;
  private String name;
  private long   init;
  private long   used;
  private long   usedPSEdenSPace;
  private long   usedPSSurvivorSpace;
  private long   usedPSOldGen;
  private long   usedCodeCashe;
  private long   usedMetaspace;
  private long   usedCompressedClassSpace;
  private long   committed;
  private long   max;
  
  
  public MemoryInfo() {
  }
  
  public MemoryInfo(String name, MemoryUsage mUsage) {
    timestamp = new Date(System.currentTimeMillis());
    this.name = name;
    init      = mUsage.getInit() ;
    max       = mUsage.getMax() ;
    used      = mUsage.getUsed() ;
    committed = mUsage.getCommitted();
  }

  public String uniqueId() { 
    return "host=" + host + ",name="+this.name+",timestamp=" + DateUtil.asCompactDateTimeId(timestamp);
  }
  
  public Date getTimestamp() { return this.timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp  = timestamp;}
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public long getInit() { return init; }
  public void setInit(long init) { this.init = init; }

  public long getUsed() { return used ; }
  public void setUsed(long used) { this.used = used; }

  public long getUsedPSEdenSPace() { return usedPSEdenSPace; }
  public void setUsedPSEdenSPace(long usedPSEdenSPace) { this.usedPSEdenSPace = usedPSEdenSPace; }

  public long getUsedPSSurvivorSpace() { return usedPSSurvivorSpace; }
  public void setUsedPSSurvivorSpace(long usedPSSurvivorSpace) { this.usedPSSurvivorSpace = usedPSSurvivorSpace; }

  public long getUsedPSOldGen() { return usedPSOldGen; }
  public void setUsedPSOldGen(long usedPSOldGen) { this.usedPSOldGen = usedPSOldGen; }

  public long getUsedCodeCashe() { return usedCodeCashe; }
  public void setUsedCodeCashe(long usedCodeCashe) { this.usedCodeCashe = usedCodeCashe; }

  public long getUsedMetaspace() { return usedMetaspace; }
  public void setUsedMetaspace(long usedMetaspace) { this.usedMetaspace = usedMetaspace; }

  public long getUsedCompressedClassSpace() { return usedCompressedClassSpace; }
  public void setUsedCompressedClassSpace(long usedCompressedClassSpace) { this.usedCompressedClassSpace = usedCompressedClassSpace; }
  
  public long getCommitted() { return committed ;}
  public void setCommitted(long committed) { this.committed = committed; }

  public long getMax() { return max; }
  public void setMax(long max) { this.max = max; }
  
  public String toString() { return getFormattedText(this) ; }
  
  static public String getFormattedText(MemoryInfo ... memoryInfo) {
    String[] header = { "Timestamp", "Host", "Name", "Init", "Max", "Used", "Committed", "PS Eden Space", 
        "PS Survivor Space", "PS Old Gen", "Code Cache", "Compressed Class Space", "Metaspace"} ;
    TabularFormater formater = new TabularFormater(header);
    for(MemoryInfo sel : memoryInfo) {
      formater.addRow(
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getHost(),
          sel.getName(),
          ByteUtil.byteToHumanReadable(sel.getInit()),
          ByteUtil.byteToHumanReadable(sel.getMax()),
          ByteUtil.byteToHumanReadable(sel.getUsed()),
          ByteUtil.byteToHumanReadable(sel.getCommitted()),
          ByteUtil.byteToHumanReadable(sel.getUsedPSEdenSPace()),
          ByteUtil.byteToHumanReadable(sel.getUsedPSSurvivorSpace()),
          ByteUtil.byteToHumanReadable(sel.getUsedPSOldGen()),
          ByteUtil.byteToHumanReadable(sel.getUsedCodeCashe()),
          ByteUtil.byteToHumanReadable(sel.getUsedCompressedClassSpace()),
          ByteUtil.byteToHumanReadable(sel.getUsedMetaspace())
      ); 
    }
    return formater.getFormattedText();
  }
}