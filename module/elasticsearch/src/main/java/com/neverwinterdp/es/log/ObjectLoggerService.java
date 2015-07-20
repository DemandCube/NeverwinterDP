package com.neverwinterdp.es.log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectLoggerService {
  private String[] connect ;
  private String   bufferBaseDir ;
  private int      queueMaxSizePerSegment;
  private Map<String, ObjectLogger<?>> loggers = new ConcurrentHashMap<String, ObjectLogger<?>>();
  private FlushThread flushThread ;
  
  public ObjectLoggerService() {
  }
  
  public ObjectLoggerService(String[] connect, String bufferBaseDir, int queueMaxSizePerSegment) throws Exception {
    init(connect, bufferBaseDir, queueMaxSizePerSegment);
  }
  
  protected void init(String[] connect, String bufferBaseDir, int queueMaxSizePerSegment) throws Exception {
    this.connect = connect;
    this.bufferBaseDir = bufferBaseDir ;
    this.queueMaxSizePerSegment = queueMaxSizePerSegment;
    flushThread = new FlushThread() ;
    flushThread.start();
  }
  
  public <T> void add(Class<T> type) throws Exception {
    String bufferDir = bufferBaseDir + "/" + type.getSimpleName().replaceAll("(.)([A-Z])", "$1-$2").toLowerCase();
    ObjectLogger<T> logger = new ObjectLogger<T>(connect, type, bufferDir, queueMaxSizePerSegment) ;
    loggers.put(type.getName(), logger);
  }
  
  public <T> void log(String id, T object) {
    ObjectLogger<T> logger = (ObjectLogger<T>) loggers.get(object.getClass().getName());
    logger.log(id, object);
  }
  
  public int flush() {
    int count = 0 ;
    for(ObjectLogger<?> logger : loggers.values()) {
      count += logger.flush();
    }
    return count;
  }
  
  public void close() {
    
  }
  
  public class FlushThread extends Thread {
    public void run() {
      try {
        while(true) {
          int count = flush() ;
          if(count == 0) {
            Thread.sleep(5000);
          }
        }
      } catch (InterruptedException e) {
      }
    }
  }
}
