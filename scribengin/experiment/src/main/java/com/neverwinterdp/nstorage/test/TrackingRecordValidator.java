package com.neverwinterdp.nstorage.test;

import java.io.IOException;
import java.util.Random;

import com.neverwinterdp.nstorage.NStorageReader;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingRecordValidator implements Runnable {
  private NStorageReader        reader;
  private TrackingRecordService trackingService;
  private int                   expectNumOfRecordPerSet;
  private int                   numOfRecordPerCommit ;
  private long                  maxWaitTime = 3000;
  private double                randomRollbackRatio = 0.25;
  private boolean               complete = false;

  public TrackingRecordValidator(NStorageReader reader, int expectNumOfRecordPerSet, int numOfRecordPerCommit) {
    this.reader                  = reader;
    this.expectNumOfRecordPerSet = expectNumOfRecordPerSet;
    this.numOfRecordPerCommit    = numOfRecordPerCommit;
  }
  
  public TrackingRecordValidator setRandomRollbackRatio(double ratio) {
    randomRollbackRatio = ratio;
    return this;
  }

  @Override
  public void run() {
    try {
      if(randomRollbackRatio > 0) runWithRandomRollback();
      else runWithoutRandomRollback(); 
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  void runWithoutRandomRollback() throws Exception {
    trackingService = new TrackingRecordService(expectNumOfRecordPerSet);
    while(!complete) {
      readWithCommit();
    }
  }
  
  void runWithRandomRollback() throws Exception {
    trackingService = new TrackingRecordService(expectNumOfRecordPerSet);
    Random rand = new Random();
    while(!complete) {
      if(rand.nextDouble() < randomRollbackRatio) readWithRollback();
      else readWithCommit();
    }
  }
  
  public void report() {
    trackingService.report();
  }
  
  void readWithCommit() throws IOException, RegistryException, InterruptedException {
    byte[] record = null ;
    int    readCount = 0;
    while(readCount < numOfRecordPerCommit && (record = reader.nextRecord(maxWaitTime)) != null) {
      TrackingRecord trackingRecord = JSONSerializer.INSTANCE.fromBytes(record, TrackingRecord.class);
      trackingService.log(trackingRecord);
      readCount++ ;
    }
    reader.prepareCommit();
    reader.completeCommit();
    if(record == null) complete = true;
  }
  
  void readWithRollback() throws IOException, RegistryException, InterruptedException {
    byte[] record    = null;
    int    readCount =    0;
    while(readCount < numOfRecordPerCommit && (record = reader.nextRecord(maxWaitTime)) != null) {
      readCount++ ;
    }
    reader.rollback();
  }
}
