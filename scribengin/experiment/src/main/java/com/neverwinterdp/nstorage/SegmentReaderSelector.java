package com.neverwinterdp.nstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.neverwinterdp.nstorage.SegmentReader.DataAvailability;
import com.neverwinterdp.registry.RegistryException;

public class SegmentReaderSelector {
  private List<SegmentReader>  allSegmentReaders;
  private List<SegmentReader>  eosSegmentReaders;
  
  private Queue<SegmentReader> activeSegmentReaders;
  private SegmentReader        currentSegmentReader;
  
  public SegmentReaderSelector() {
    allSegmentReaders    = new ArrayList<>();
    eosSegmentReaders    = new ArrayList<>();
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
      if(prevSegId + 1 != segId) {
        throw new IOException("The segment is not in sequence. previous id =  " + prevSegId + ", id = " + segId);
      }
    }
    allSegmentReaders.add(segmentReader);
    DataAvailability dataAvailability = segmentReader.getDataAvailability();
    if(dataAvailability == DataAvailability.EOS) {
      eosSegmentReaders.add(segmentReader);
    } else {
      activeSegmentReaders.add(segmentReader);
    }
  }
  
  public SegmentReader select() throws IOException, RegistryException {
    if(currentSegmentReader != null) {
      DataAvailability dataAvailability = currentSegmentReader.getDataAvailability(); 
      if(dataAvailability == DataAvailability.YES) {
        return currentSegmentReader;
      } else if(dataAvailability == DataAvailability.EOS) {
        eosSegmentReaders.add(currentSegmentReader);
        currentSegmentReader = null;
      } else {
        activeSegmentReaders.add(currentSegmentReader);
        currentSegmentReader = null;
      }
    }
    Iterator<SegmentReader> i = activeSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader segReader = i.next();
      DataAvailability dataAvailability = segReader.getDataAvailability(); 
      if(dataAvailability == DataAvailability.YES) {
        i.remove();
        currentSegmentReader = segReader;
        return segReader;
      } else if(dataAvailability == DataAvailability.EOS) {
        eosSegmentReaders.add(segReader);
        i.remove();
      }
    }
    return null;
  }
}
