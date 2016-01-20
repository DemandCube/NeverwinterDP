package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

abstract public class SSMReader {
  protected SSMRegistry           registry;
  protected SSMReaderDescriptor   readerDescriptor;
  private   SegmentReaderIterator segmentReaderIterator;
  
  protected void init(String clientId, SSMRegistry registry) throws RegistryException, IOException {
    this.registry    = registry;
    readerDescriptor = registry.getOrCreateReader(clientId);
    
    segmentReaderIterator = new SegmentReaderIterator();
    List<String> currentReadSegments = registry.getSegmentReadDescriptors(readerDescriptor);
    if(currentReadSegments.size() > 0) {
      for(int i = 0; i < currentReadSegments.size(); i++) {
        SegmentReadDescriptor segRead = registry.getSegmentReadDescriptor(readerDescriptor, currentReadSegments.get(i));
        SegmentDescriptor segment = registry.getSegmentBySegmentId(segRead.getSegmentId());
        segmentReaderIterator.add(createSegmentReader(segment, segRead));
      }
    } else {
      List<String> segments = registry.getSegments() ;
      if(segments.size() > 0) {
        String segmentId = segments.get(0);
        SegmentDescriptor segment = registry.getSegmentBySegmentId(segmentId);
        SegmentReadDescriptor segRead = registry.createSegmentReadDescriptor(readerDescriptor, segment);
        segmentReaderIterator.add(createSegmentReader(segment, segRead));
      }
    }
  }
  
  protected void init(String clientId, SSMRegistry registry, int segId, long recordPos) throws RegistryException, IOException {
    this.registry    = registry;
    readerDescriptor = registry.getOrCreateReader(clientId);
    segmentReaderIterator = new SegmentReaderIterator();
    if(readerDescriptor.getLastReadSegmentId() != null) {
      throw new RegistryException(ErrorCode.Unknown, "Can seek to a segment position only for a new reader");
    } else {
      String segmentIdName = SegmentDescriptor.toSegmentId(segId);
      SegmentDescriptor segment = registry.getSegmentBySegmentId(segmentIdName);
      SegmentReadDescriptor segRead = registry.createSegmentReadDescriptor(readerDescriptor, segment);
      SegmentReader segmentReader = createSegmentReader(segment, segRead);
      int idx = 0;
      while(idx < recordPos && segmentReader.nextRecord() != null) {
        idx++;
      }
      segmentReaderIterator.add(segmentReader);
    }
  }
  
  public byte[] nextRecord(long maxWait) throws IOException, RegistryException, InterruptedException {
    byte[] record = segmentReaderIterator.nextRecord();
    if(record != null) return record;
    
    long stopTime = System.currentTimeMillis() + maxWait;
    long checkPeriod = 500;
    if(maxWait < checkPeriod) checkPeriod = maxWait;
    while(System.currentTimeMillis() < stopTime) {
      String lastReadSegment = readerDescriptor.getLastReadSegmentId();
      SegmentDescriptor nextSegment = null;
      if(lastReadSegment != null) {
        int lastReadSegmentId = Integer.parseInt(lastReadSegment.substring(lastReadSegment.lastIndexOf('-') + 1));
        nextSegment = registry.getNextSegmentDescriptor(lastReadSegmentId);
      } else {
        List<String> segments = registry.getSegments() ; 
        if(segments.size() > 0) nextSegment = registry.getSegmentBySegmentId(segments.get(0));
      }
      if(nextSegment != null) {
        SegmentReadDescriptor nextSegmentRead = registry.createSegmentReadDescriptor(readerDescriptor, nextSegment);
        segmentReaderIterator.add(createSegmentReader(nextSegment, nextSegmentRead));
      }
      record = segmentReaderIterator.nextRecord();
      if(record != null) return record;
      Thread.sleep(checkPeriod);
    }
    return null;
  }
  
  public void prepareCommit() throws IOException, RegistryException {
    Transaction transaction = registry.getRegistry().getTransaction();
    segmentReaderIterator.prepareCommit(transaction);
    transaction.commit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    Transaction transaction = registry.getRegistry().getTransaction();
    segmentReaderIterator.completeCommit(transaction);
    transaction.commit();
  }

  public void rollback() throws IOException, RegistryException {
    Transaction transaction = registry.getRegistry().getTransaction();
    segmentReaderIterator.rollback(transaction);
    transaction.commit();
  }
  
  public void close() throws IOException, RegistryException {
    segmentReaderIterator.close();
  }
  
  public void closeAndRemove() throws IOException, RegistryException {
    segmentReaderIterator.close();
    registry.removeReader(readerDescriptor);
  }
  
  abstract protected SegmentReader createSegmentReader(SegmentDescriptor segment, SegmentReadDescriptor segReadDescriptor) throws RegistryException, IOException ;
}