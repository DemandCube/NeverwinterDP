package com.neverwinterdp.kafka.producer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class WaittingAckProducerRecordHolder<K, V> {
  private int maxSize = 5000;
  private LinkedHashMap<Long, WaittingAckProducerRecord<K,V>> buffer = new LinkedHashMap<Long, WaittingAckProducerRecord<K, V>>();

  public int size() { return buffer.size(); }
  
  synchronized public void clear() { buffer.clear(); }
  
  synchronized public List<WaittingAckProducerRecord<K,V>> getNeedToResendRecords() {
    List<WaittingAckProducerRecord<K,V>> holder = new ArrayList<WaittingAckProducerRecord<K,V>>();
    for(WaittingAckProducerRecord<K,V> sel : buffer.values()) {
      if(sel.isNeedToResend()) holder.add(sel);
    }
    return holder;
  }
  
  synchronized public void add(WaittingAckProducerRecord<K, V> record, long timeout) throws Exception {
    if(buffer.size() >= maxSize) {
      waitForAvailableBuffer(timeout);
    }
    buffer.put(record.getId(), record);
  }
  
  synchronized public WaittingAckProducerRecord<K,V> onSuccess(long recordId) {
    WaittingAckProducerRecord<K,V> record = null ;
    record = buffer.remove(recordId);
    notifyForAvailableBuffer();
    return record;
  }
  
  synchronized public WaittingAckProducerRecord<K,V> onFail(long recordId) {
    WaittingAckProducerRecord<K,V> record = null ;
    record = buffer.get(recordId);
    record.setNeedToResend(true);
    return record;
  }

  synchronized void waitForAvailableBuffer(long waitTime) throws InterruptedException {
    wait(waitTime);
  }
  
  synchronized void waitForEmptyBuffer(long waitTime) throws Exception {
    long startTime = System.currentTimeMillis();
    while(buffer.size() > 0) {
      long remainTime = waitTime - (System.currentTimeMillis() - startTime);
      if(remainTime <= 0) break;
      wait(remainTime);
    }
    if(buffer.size() > 0) { 
      throw new Exception("Not all messages are sent and ack after " + waitTime + "ms");
    }
  }

  synchronized void notifyForAvailableBuffer() {
    notifyAll();
  }
}
