package com.neverwinterdp.nstorage;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

abstract public class NStorageReader {
  protected NStorageRegistry         registry;
  protected NStorageReaderDescriptor readerDescriptor;
  private   SegmentReaderSelector    segmentReaderSelector;
  
  
  protected void init(String clientId, NStorageRegistry registry) throws RegistryException, IOException {
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
    SegmentReader segReader = select();
    if(segReader != null) return segReader.nextRecord();
    long stopTime = System.currentTimeMillis() + maxWait;
    while(System.currentTimeMillis() < stopTime) {
      Thread.sleep(1000);
      segReader = select();
      if(segReader != null) return segReader.nextRecord();
    }
    return null;
  }
  
  public SegmentReader select() throws IOException, RegistryException {
    SegmentReader segReader = segmentReaderSelector.select();
    if(segReader != null) return segReader;
    if(segmentReaderSelector.countActiveSegmentReader() == 0) {
      SegmentDescriptor lastSegment = segmentReaderSelector.getLastSegmentDescriptor();
      if(lastSegment != null) {
        SegmentDescriptor nextSegment = registry.getNextSegmentDescriptor(lastSegment);
        if(nextSegment != null) {
          SegmentReadDescriptor nextSegmentRead = registry.createSegmentReadDescriptor(readerDescriptor, nextSegment);
          segmentReaderSelector.add(createSegmentReader(nextSegment, nextSegmentRead));
          System.err.println("create " + nextSegmentRead.getSegmentId());
          return select();
        }
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
  }

  
  abstract protected SegmentReader createSegmentReader(SegmentDescriptor segment, SegmentReadDescriptor segReadDescriptor) throws RegistryException, IOException ;
}