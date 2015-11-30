package com.neverwinterdp.nstorage.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrackingRecordService {
  private int expectNumOfRecordPerSet ;
  private Map<String, TrackingRecordBitSet> byWriterTrackingSet = new HashMap<>();

  public TrackingRecordService(int expectNumOfRecordPerSet) {
    this.expectNumOfRecordPerSet = expectNumOfRecordPerSet;
  }
  
  public void log(TrackingRecord record) {
    TrackingRecordBitSet byWriterSet = byWriterTrackingSet.get(record.getWriter());
    if(byWriterSet == null) {
      byWriterSet = new TrackingRecordBitSet(record.getWriter(), record.getChunkId(), expectNumOfRecordPerSet);
      byWriterTrackingSet.put(record.getWriter(), byWriterSet);
    }
    byWriterSet.log(record);
  }
  
  public void report() {
    String[] groupBy = new String[byWriterTrackingSet.size()]; 
    byWriterTrackingSet.keySet().toArray(groupBy);
    Arrays.sort(groupBy);
    
    List<TrackingRecordReport> reports = new ArrayList<>();
    for(int i = 0; i < groupBy.length; i++) {
      TrackingRecordBitSet byWriterSet = byWriterTrackingSet.get(groupBy[i]);
      reports.add(byWriterSet.updateAndGetReport());
    }
    System.out.println(TrackingRecordReport.getFormattedReport("Tracking Record Report", reports));
  }
}
