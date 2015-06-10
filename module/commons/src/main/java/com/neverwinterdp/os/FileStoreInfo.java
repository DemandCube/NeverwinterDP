package com.neverwinterdp.os;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileStore;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.ByteUtil;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

@SuppressWarnings("serial")
public class FileStoreInfo implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp ;
  private String host;
  private String name ;
  private long   total ;
  private long   used  ;
  private long   available;

  public FileStoreInfo() {} 
  
  public FileStoreInfo(FileStore store) throws IOException {
    timestamp = new Date();
    name = store.name();
    total = store.getTotalSpace();
    used =  store.getTotalSpace() - store.getUnallocatedSpace();
    available = store.getUsableSpace();
  } 
  
  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public long getTotal() { return total; }
  public void setTotal(long total) { this.total = total; }
  
  public long getUsed() { return used; }
  public void setUsed(long used) { this.used = used; }
  
  public long getAvailable() { return available; }
  public void setAvailable(long available) { this.available = available; }

  static public String getFormattedText(FileStoreInfo ... info) {
    String[] header = {"Timestamp", "Host", "Name",  "Total", "Used", "Available"} ;
    TabularFormater formatter = new TabularFormater(header) ;
    for(FileStoreInfo sel : info) {
      formatter.addRow(
          DateUtil.asCompactDateTime(sel.getTimestamp()),
          sel.getHost(),
          sel.getName(), 
          ByteUtil.byteToHumanReadable(sel.getTotal()), 
          ByteUtil.byteToHumanReadable(sel.getUsed()),
          ByteUtil.byteToHumanReadable(sel.getAvailable())
       );
    }
    return formatter.getFormattedText() ;
  }
}
