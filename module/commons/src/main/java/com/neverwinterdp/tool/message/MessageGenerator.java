package com.neverwinterdp.tool.message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.util.JSONSerializer;

public interface MessageGenerator {
  
  public byte[] nextMessage(String partition, int messageSize);
  
  //TODO remove after testing
  public Map<String, AtomicInteger> getMessageTrackers();
  
  public int getCurrentSequenceId(String partition);
  
  static public class DefaultMessageGenerator implements MessageGenerator {
    private Map<String, AtomicInteger> idTrackers = new HashMap<>() ;
    
    @Override
    public byte[] nextMessage(String partition, int messageSize) {
      AtomicInteger idTracker = getIdTracker(partition) ;
      Message message = new Message(partition, idTracker.getAndIncrement(), messageSize) ;
      return JSONSerializer.INSTANCE.toBytes(message) ;
    }
    
    public int getCurrentSequenceId(String partition) {
      AtomicInteger idTracker = getIdTracker(partition) ;
      return idTracker.get();
    }
    
    AtomicInteger getIdTracker(String partition) {
      AtomicInteger idTracker = idTrackers.get(partition) ;
      if(idTracker != null) return idTracker; 
      synchronized(idTrackers) {
        idTracker = idTrackers.get(partition) ;
        if(idTracker != null) return idTracker;
        idTracker = new AtomicInteger() ;
        idTrackers.put(partition, idTracker) ;
        return  idTracker;
      }
    }

    @Override
    public Map<String, AtomicInteger> getMessageTrackers() { return idTrackers; }
  };
}
