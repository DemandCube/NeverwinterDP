package com.neverwinterdp.tool.message;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.util.text.TabularFormater;

public class BitSetMessageTracker {
  private int expectNumOfMessage ;
  private Map<String, BitSetPartitionMessageTracker> partitions = new HashMap<>();
  
  public BitSetMessageTracker(int expectNumOfMessage) {
    this.expectNumOfMessage = expectNumOfMessage;
  }
  
  synchronized public void log(String partition, int index) {
    BitSetPartitionMessageTracker pTracker = partitions.get(partition) ;
    if(pTracker == null) {
      pTracker = new BitSetPartitionMessageTracker(expectNumOfMessage);
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
    TabularFormater formater = new TabularFormater("Partition", "Expect", "Lost", "Duplicated");
    for(Map.Entry<String, BitSetPartitionMessageTracker> entry : partitions.entrySet()) {
      BitSetPartitionMessageTracker tracker = entry.getValue();
      formater.addRow(entry.getKey(), tracker.getExpect(), tracker.getLostCount(), tracker.getDuplicatedCount());
    }
    return formater.getFormattedText();
  }
  
  static public class BitSetPartitionMessageTracker {
    private BitSet bitSet ;
    private int duplicatedCount = 0;
    private int numOfBits ;
    
    public BitSetPartitionMessageTracker(int nBits) {
      this.numOfBits = nBits ;
      bitSet = new BitSet(nBits) ;
    }
    
    public void log(int idx) {
      if(idx > numOfBits) {
        throw new RuntimeException("the index is bigger than expect num of bits " + numOfBits);
      }
      if(bitSet.get(idx)) duplicatedCount++ ;
      bitSet.set(idx, true);
    }
    
    public int getExpect() { return numOfBits; }
    
    public int getDuplicatedCount() { return this.duplicatedCount ; }
   
    public int getLostCount() {
      int lostCount = 0;
      for(int i = 0; i < numOfBits; i++) {
        if(!bitSet.get(i)) lostCount++ ;
      }
      return lostCount;
    }
  }
}