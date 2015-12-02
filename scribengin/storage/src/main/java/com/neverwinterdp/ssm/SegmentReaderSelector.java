package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.ssm.SegmentReader.DataAvailability;

public class SegmentReaderSelector {
  private List<SegmentReader>       allSegmentReaders;
  private LinkedList<SegmentReader> activeSegmentReaders;
  
  public SegmentReaderSelector() {
    allSegmentReaders    = new ArrayList<>();
    activeSegmentReaders = new LinkedList<>();
  }
  
  public int countActiveSegmentReader() { return activeSegmentReaders.size(); }
  
  public SegmentDescriptor getLastSegmentDescriptor() {
    if(allSegmentReaders.size() == 0) return null;
    SegmentReader lastReader = allSegmentReaders.get(allSegmentReaders.size() - 1);
    return lastReader.getSegmentDescriptor();
  }
  
  public void add(SegmentReader segmentReader) throws IOException, RegistryException {
    if(allSegmentReaders.size() > 0) {
      SegmentReader prevSegReader = allSegmentReaders.get(allSegmentReaders.size() - 1);
      int prevSegId = prevSegReader.getSegmentDescriptor().getId(); 
      int segId     = segmentReader.getSegmentDescriptor().getId();
      if(prevSegId + 1 > segId) {
        throw new IOException("The segment is not in sequence. previous id =  " + prevSegId + ", id = " + segId);
      }
    }
    allSegmentReaders.add(segmentReader);
    DataAvailability dataAvailability = segmentReader.updateAndGetDataAvailability();
    if(dataAvailability != DataAvailability.EOS) {
      activeSegmentReaders.add(segmentReader);
    }
  }
  
  public SegmentReader select() throws IOException, RegistryException {
    if(activeSegmentReaders.size() == 0) return null;
    Iterator<SegmentReader> i = activeSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader segReader = i.next();
      DataAvailability dataAvailability = segReader.updateAndGetDataAvailability(); 
      if(dataAvailability == DataAvailability.YES) {
        return segReader;
      } else if(dataAvailability == DataAvailability.EOS) {
        i.remove();
      }
    }
    
    i = activeSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader segReader = i.next();
      segReader.updateSegmentDescriptor();
      DataAvailability dataAvailability = segReader.updateAndGetDataAvailability(); 
      if(dataAvailability == DataAvailability.YES) {
        return segReader;
      } else if(dataAvailability == DataAvailability.EOS) {
        i.remove();
      }
    }
    return null;
  }
  
  public void prepareCommit(Transaction transaction) throws IOException, RegistryException {
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.prepareCommit(transaction);
    }
  }
  
  public void completeCommit(Transaction transaction) throws IOException, RegistryException {
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.completeCommit(transaction);
      if(reader.isComplete()) i.remove();
    }
  }
  
  public void rollback(Transaction transaction) throws IOException, RegistryException {
    activeSegmentReaders.clear();
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.rollback(transaction);
      DataAvailability availability  = reader.updateAndGetDataAvailability() ;
      if(availability == DataAvailability.YES || availability == DataAvailability.YES) {
        activeSegmentReaders.add(reader);
      }
    }
  }
}
