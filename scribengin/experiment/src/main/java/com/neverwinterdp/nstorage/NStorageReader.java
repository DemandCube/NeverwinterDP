package com.neverwinterdp.nstorage;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorageReader {
  protected NStorageRegistry         registry;
  protected NStorageReaderDescriptor readerDescriptor;
  
  private   SegmentReaderSelector    segmentReaderSelector;
  
  
  public NStorageReader(String clientId, NStorageRegistry registry) throws RegistryException, IOException {
    this.registry    = registry;
    readerDescriptor = registry.getOrCreateReader(clientId);
    
    segmentReaderSelector = new SegmentReaderSelector();
    List<String> currentReadSegments = registry.getSegmentReadDescriptors(readerDescriptor);
    for(int i = 0; i < currentReadSegments.size(); i++) {
      SegmentReadDescriptor segRead = registry.getSegmentReadDescriptor(readerDescriptor, currentReadSegments.get(i));
      SegmentDescriptor segment = registry.getSegmentBySegmentId(segRead.getSegmentId());
      segmentReaderSelector.add(createSegmentReader(segment, segRead));
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
      if(lastSegment == null) {
        SegmentDescriptor nextSegment = registry.getNextSegmentDescriptor(lastSegment);
        if(nextSegment != null) {
          SegmentReadDescriptor nextSegmentRead = registry.createSegmentReadDescriptor(readerDescriptor, nextSegment);
          segmentReaderSelector.add(createSegmentReader(nextSegment, nextSegmentRead));
          return select();
        }
      }
    }
    return null;
  }
  
  public List<byte[]> nextRecord(int maxRetrieve, long maxWait) throws IOException, RegistryException {
    return null;
  }
  
  public void prepareCommit() throws IOException {
  }
  
  public void completeCommit() throws IOException {
  }

  abstract protected SegmentReader createSegmentReader(SegmentDescriptor segment, SegmentReadDescriptor segReadDescriptor) throws RegistryException, IOException ;
}