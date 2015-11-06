package com.neverwinterdp.scribengin.storage.hdfs;

import java.text.SimpleDateFormat;
import java.util.Date;

public interface HDFSStoragePartitioner {
  public String getCurrentPartition();
  
  static public class Default implements HDFSStoragePartitioner {
    @Override
    public String getCurrentPartition() { return "storage"; }
  }
  
  static public class Hourly implements HDFSStoragePartitioner {
    final static public SimpleDateFormat PARTITION_TIME_FORMATER = new SimpleDateFormat("yyyy-MM-dd@HH") ;
    
    @Override
    public String getCurrentPartition() { 
      return "storage-" + PARTITION_TIME_FORMATER.format(new Date()); 
    }
  }
}
