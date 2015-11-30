package com.neverwinterdp.nstorage.test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.nstorage.NStorageWriter;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;

public class RandomCommitRollbackTRGenerator implements Runnable {
  private int            numOfRecordPerCommit = 10;
  private int            numOfCommit          = 10;
  private AtomicInteger  idTracker            = new AtomicInteger();
  private NStorageWriter writer;
  private String         chunkId              = "chunk-0";

  public RandomCommitRollbackTRGenerator(NStorageWriter writer, int numOfCommit, int numOfRecordPerCommit) {
    this.numOfCommit          = numOfCommit;
    this.numOfRecordPerCommit = numOfRecordPerCommit;
    this.writer = writer;
  }
  
  public RandomCommitRollbackTRGenerator set1MBMaxSegmentSize() {
    writer.
      setMaxSegmentSize(1024 * 1024).
      setMaxBufferSize(512 * 1024);
    return this;
  }
  
  @Override
  public void run() {
    try {
      doRun();
    } catch (RegistryException | IOException e) {
      e.printStackTrace();
    }
  }
  
  void doRun() throws RegistryException, IOException {
    int commitCount = 0;
    Random rand = new Random();
    while(commitCount < numOfCommit) {
      if(rand.nextInt(3) == 1) {
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