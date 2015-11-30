package com.neverwinterdp.nstorage.test;

import java.util.BitSet;

public class TrackingRecordBitSet {

  private BitSet bitSet;
  private int    duplicatedCount = 0;
  private int    numOfBits;
  private int    trackProgress   = -1;
  private int    logCount;

  private TrackingRecordReport report;
  
  public TrackingRecordBitSet(String writerId, String chunkId, int expectNumOfRecord) {
    numOfBits = expectNumOfRecord ;
    bitSet = new BitSet(expectNumOfRecord) ;
    report = new TrackingRecordReport();
    report.setWriterId(writerId);
    report.setChunkId(chunkId);
    report.setNumOfRecord(expectNumOfRecord);
  }
  
  synchronized public int log(TrackingRecord record) {
    int idx = record.getTrackingId();
    if(idx > numOfBits) {
      throw new RuntimeException("the index is bigger than expect num of bits " + numOfBits);
    }
    if(idx > trackProgress) trackProgress = idx;
    if(bitSet.get(idx)) duplicatedCount++ ;
    bitSet.set(idx, true);
    logCount++ ;
    return logCount;
  }
  
  
  synchronized public TrackingRecordReport updateAndGetReport() {
    int lostCount = 0;
    int noLostTo = -1;
    for(int i = 0; i < trackProgress; i++) {
      if(!bitSet.get(i)) {
        if(noLostTo < 0) noLostTo = i;
        lostCount++ ;
      }
    }
    if(noLostTo < 0) noLostTo = trackProgress;
    report.setProgress(trackProgress + 1);
    report.setNoLostTo(noLostTo + 1);
    report.setLostCount(lostCount);
    report.setDuplicatedCount(duplicatedCount);
    return report;
  }
}