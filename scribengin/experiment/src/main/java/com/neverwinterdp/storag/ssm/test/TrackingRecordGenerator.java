package com.neverwinterdp.storag.ssm.test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.storage.ssm.SSMWriter;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingRecordGenerator implements Runnable {
  private int            numOfRecordPerCommit = 10;
  private int            numOfCommit          = 10;
  private AtomicInteger  idTracker            = new AtomicInteger();
  private SSMWriter writer;
  private String         chunkId              = "chunk-0";
  private double         randomRollbackRatio  = 0.25;
  
  public TrackingRecordGenerator(SSMWriter writer, int numOfCommit, int numOfRecordPerCommit) {
    this.numOfCommit          = numOfCommit;
    this.numOfRecordPerCommit = numOfRecordPerCommit;
    this.writer = writer;
  }
  
  public TrackingRecordGenerator set1MBMaxSegmentSize() {
    writer.
      setMaxSegmentSize(1024 * 1024).
      setMaxBufferSize(512 * 1024);
    return this;
  }
  
  public TrackingRecordGenerator set25MBMaxSegmentSize() {
    writer.
      setMaxSegmentSize(25 * 1024 * 1024).
      setMaxBufferSize(  5 * 1024 * 1024);
    return this;
  }
  
  public TrackingRecordGenerator setRandomRollbackRatio(double ratio) {
    randomRollbackRatio = ratio;
    return this;
  }
  
  @Override
  public void run() {
    try {
      if(randomRollbackRatio > 0) runWithRandomRollback();
      else                        runWithoutRandomRollback();
    } catch (RegistryException | IOException e) {
      e.printStackTrace();
    }
  }
  
  void runWithoutRandomRollback() throws RegistryException, IOException {
    int commitCount = 0;
    while(commitCount < numOfCommit) {
      writeWithCommit();
      commitCount++ ;
    }
    writer.closeAndRemove();
  }
  
  void runWithRandomRollback() throws RegistryException, IOException {
    int commitCount = 0;
    Random rand = new Random();
    while(commitCount < numOfCommit) {
      if(rand.nextDouble() < randomRollbackRatio) {
        writeWithRollback();
      } else {
        writeWithCommit();
        commitCount++ ;
      }
    }
    writer.closeAndRemove();
  }
  
  public void writeWithCommit() throws RegistryException, IOException {
    for(int j = 1; j <= numOfRecordPerCommit; j++) {
      TrackingRecord record = new TrackingRecord(writer.getWriterId(), chunkId,  idTracker.getAndIncrement(), 128);
      writer.write(JSONSerializer.INSTANCE.toBytes(record));
    }
    writer.prepareCommit();
    writer.completeCommit();
  }
  
  public void writeWithRollback() throws RegistryException, IOException {
    for(int j = 1; j <= numOfRecordPerCommit; j++) {
      TrackingRecord record = new TrackingRecord(writer.getWriterId(), chunkId, 0, 128);
      writer.write(JSONSerializer.INSTANCE.toBytes(record));
    }
    writer.rollback();
  }
  
  public void writerCloseAndRemove() throws IOException, RegistryException {
    writer.closeAndRemove();
  }
}