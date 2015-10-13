package com.neverwinterdp.tool.message;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.util.text.TabularFormater;

public class BitSetMessageTracker {
  private int expectNumOfMessagePerPartition ;
  private Map<String, BitSetPartitionMessageTracker> partitions = new HashMap<>();
  
  public BitSetMessageTracker(int expectNumOfMessage) {
    this.expectNumOfMessagePerPartition = expectNumOfMessage;
  }
  
  synchronized public void log(String partition, int index) {
    BitSetPartitionMessageTracker pTracker = partitions.get(partition) ;
    if(pTracker == null) {
      pTracker = new BitSetPartitionMessageTracker(expectNumOfMessagePerPartition);
      partitions.put(partition, pTracker);
    }
    pTracker.log(index);;
  }
 
  public String[] getPartitions() {
    String[] names = new String[partitions.size()];
    partitions.keySet().toArray(names);
    return names;
  }
  
  public BitSetPartitionMessageTracker getPartitionTracker(String name) { return partitions.get(name); }
  
  public String getFormatedReport() {
    TabularFormater formater = new TabularFormater("Partition", "Expect", "Progress", "Lost", "Duplicated");
    for(Map.Entry<String, BitSetPartitionMessageTracker> entry : partitions.entrySet()) {
      BitSetPartitionMessageTracker tracker = entry.getValue();
      BitSetPartitionMessageReport report = tracker.getReport();
      formater.addRow(entry.getKey(), report.getNumOfBits(), report.getTrackProgress(), report.getLostCount(), report.getDuplicatedCount());
    }
    return formater.getFormattedText();
  }
  
  static public class BitSetPartitionMessageTracker {
    private BitSet bitSet;
    private int    duplicatedCount = 0;
    private int    numOfBits;
    private int    trackProgress;
    
    public BitSetPartitionMessageTracker(int nBits) {
      this.numOfBits = nBits ;
      bitSet = new BitSet(nBits) ;
    }
    
    synchronized public void log(int idx) {
      if(idx > numOfBits) {
        throw new RuntimeException("the index is bigger than expect num of bits " + numOfBits);
      }
      if(idx > trackProgress) trackProgress = idx;
      if(bitSet.get(idx)) duplicatedCount++ ;
      bitSet.set(idx, true);
    }
    
    synchronized public BitSetPartitionMessageReport getReport() {
      BitSetPartitionMessageReport report = new BitSetPartitionMessageReport();
      int lostCount = 0;
      int noLostTo = -1;
      for(int i = 0; i < trackProgress; i++) {
        if(!bitSet.get(i)) {
          if(noLostTo < 0) noLostTo = i;
          lostCount++ ;
        }
      }
      report.setNumOfBits(numOfBits);
      report.setTrackProgress(trackProgress);
      report.setNoLostTo(noLostTo);
      report.setLostCount(lostCount);
      report.setDuplicatedCount(duplicatedCount);
      return report;
    }
    
    public int getLostCount() {
      int lostCount = 0;
      for(int i = 0; i < numOfBits; i++) {
        if(!bitSet.get(i)) lostCount++ ;
      }
      return lostCount;
    }
  }
  
  static public class BitSetPartitionMessageReport {
    private int numOfBits;
    private int trackProgress;
    private int noLostTo;
    private int lostCount       = 0;
    private int duplicatedCount = 0;

    public int getNumOfBits() { return numOfBits; }
    public void setNumOfBits(int numOfBits) { this.numOfBits = numOfBits; }
    
    public int getTrackProgress() { return trackProgress ; }
    public void setTrackProgress(int trackProgress) { this.trackProgress = trackProgress;}

    public int getNoLostTo() { return noLostTo; }
    public void setNoLostTo(int pos) { this.noLostTo = pos; }
    public int getLostCount() { return lostCount; }
    public void setLostCount(int lostCount) { this.lostCount = lostCount;}
    
    public int getDuplicatedCount() { return duplicatedCount; }
    public void setDuplicatedCount(int duplicatedCount) { this.duplicatedCount = duplicatedCount; }
  }
}