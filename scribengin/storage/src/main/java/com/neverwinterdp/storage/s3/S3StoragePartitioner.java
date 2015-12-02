package com.neverwinterdp.storage.s3;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public interface S3StoragePartitioner {
  public String getCurrentPartition();
  
  static public class Default implements S3StoragePartitioner {
    @Override
    public String getCurrentPartition() { return "storage"; }
  }
  
  static public class Hourly implements S3StoragePartitioner {
    final static public SimpleDateFormat PARTITION_TIME_FORMATER = new SimpleDateFormat("yyyy-MM-dd-HH00") ;
    
    @Override
    public String getCurrentPartition() { 
      return "storage-" + PARTITION_TIME_FORMATER.format(new Date()); 
    }
  }
  
  static public class Every15Min implements S3StoragePartitioner {
    final static public SimpleDateFormat PARTITION_TIME_FORMATER = new SimpleDateFormat("yyyy-MM-dd-HHmm") ;
    
    @Override
    public String getCurrentPartition() {
      Calendar cal = Calendar.getInstance();
      cal.set(Calendar.MINUTE, (cal.get(Calendar.MINUTE)/15 * 15));
      return "storage-" + PARTITION_TIME_FORMATER.format(cal.getTime()); 
    }
  }
}
