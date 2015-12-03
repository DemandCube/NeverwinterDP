package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

abstract public class SSMReader {
  protected SSMRegistry         registry;
  protected SSMReaderDescriptor readerDescriptor;
  private   SegmentReaderSelector    segmentReaderSelector;
  private   SegmentReader            currentSegmentReader;
  
  
  protected void init(String clientId, SSMRegistry registry) throws RegistryException, IOException {
    this.registry    = registry;
    readerDescriptor = registry.getOrCreateReader(clientId);
    
    segmentReaderSelector = new SegmentReaderSelector();
    List<String> currentReadSegments = registry.getSegmentReadDescriptors(readerDescriptor);
    if(currentReadSegments.size() > 0) {
      for(int i = 0; i < currentReadSegments.size(); i++) {
        SegmentReadDescriptor segRead = registry.getSegmentReadDescriptor(readerDescriptor, currentReadSegments.get(i));
        SegmentDescriptor segment = registry.getSegmentBySegmentId(segRead.getSegmentId());
        segmentReaderSelector.add(createSegmentReader(segment, segRead));
      }
    } else {
      List<String> segments = registry.getSegments() ;
      if(segments.size() > 0) {
        String segmentId = segments.get(0);
        SegmentDescriptor segment = registry.getSegmentBySegmentId(segmentId);
        SegmentReadDescriptor segRead = registry.createSegmentReadDescriptor(readerDescriptor, segment);
        segmentReaderSelector.add(createSegmentReader(segment, segRead));
      }
    }
  }
  
  public byte[] nextRecord(long maxWait) throws IOException, RegistryException, InterruptedException {
    if(currentSegmentReader != null && currentSegmentReader.hasAvailableData()) {
      return currentSegmentReader.nextRecord();
    }
    long stopTime = System.currentTimeMillis() + maxWait;
    while(System.currentTimeMillis() < stopTime) {
      currentSegmentReader = select();
      if(currentSegmentReader != null) {
        return currentSegmentReader.nextRecord();
      } else {
        Thread.sleep(1000);
      }
    }
    return null;
  }
  
  public SegmentReader select() throws IOException, RegistryException {
    SegmentReader segReader = segmentReaderSelector.select();
    if(segReader != null) return segReader;
    
    if(segmentReaderSelector.countActiveSegmentReader() == 0) {
      String lastReadSegment = readerDescriptor.getLastReadSegmentId();
      int lastReadSegmentId = Integer.parseInt(lastReadSegment.substring(lastReadSegment.lastIndexOf('-') + 1));
      SegmentDescriptor nextSegment = registry.getNextSegmentDescriptor(lastReadSegmentId);
      if(nextSegment != null) {
        SegmentReadDescriptor nextSegmentRead = registry.createSegmentReadDescriptor(readerDescriptor, nextSegment);
        segmentReaderSelector.add(createSegmentReader(nextSegment, nextSegmentRead));
        return select();
      }
    }
    return null;
  }
  
  public List<byte[]> nextRecord(int maxRetrieve, long maxWait) throws IOException, RegistryException {
    return null;
  }
  
  public void prepareCommit() throws IOException, RegistryException {
    Transaction transaction = registry.getRegistry().getTransaction();
    segmentReaderSelector.prepareCommit(transaction);
    transaction.commit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    Transaction transaction = registry.getRegistry().getTransaction();
    segmentReaderSelector.completeCommit(transaction);
    transaction.commit();
  }

  public void rollback() throws IOException, RegistryException {
    Transaction transaction = registry.getRegistry().getTransaction();
    segmentReaderSelector.rollback(transaction);
    transaction.commit();
  }
  
  public void close() throws IOException, RegistryException {
    segmentReaderSelector.close();
  }
  
  abstract protected SegmentReader createSegmentReader(SegmentDescriptor segment, SegmentReadDescriptor segReadDescriptor) throws RegistryException, IOException ;
}