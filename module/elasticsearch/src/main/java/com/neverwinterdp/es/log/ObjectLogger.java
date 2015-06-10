package com.neverwinterdp.es.log;

import java.io.IOException;
import java.io.Serializable;

import org.elasticsearch.ElasticsearchException;

import com.neverwinterdp.buffer.chronicle.MultiSegmentQueue;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.text.StringUtil;

public class ObjectLogger<T> {
  private String[] connect ;
  private String   indexName ;
  private Class<T> objectType ;
  
  private MultiSegmentQueue<LogWithId<T>> queue ; 
  private boolean  queueError = false ;
  private ESObjectClient<T> esObjectClient ;
  
  public ObjectLogger(String[] connect, Class<T> objectType, String queueBufferDir, int queueMaxSizePerSegment) throws Exception {
    this.connect = connect;
    this.indexName = "log-" + objectType.getSimpleName().replaceAll("(.)([A-Z])", "$1-$2").toLowerCase();
    this.objectType = objectType;
    this.queue = new MultiSegmentQueue<LogWithId<T>>(queueBufferDir, queueMaxSizePerSegment) ;
  }
  
  public ObjectLogger<T> setConnects(String connects) { 
    this.connect = StringUtil.toStringArray(connects) ; 
    return this;
  }
  
  public ObjectLogger<T> setIndexName(String indexName) { 
    this.indexName = indexName ;
    return this;
  }
  
  public void log(String id, T object) {
    if(queueError) return ;
    try {
      LogWithId<T> log = new LogWithId<T>(id, object);
      queue.writeObject(log) ;
    } catch (Exception e) {
      queueError = true ;
      e.printStackTrace();
    }
  }

  synchronized public void close() throws IOException { 
    queue.close(); 
  }
  
  synchronized public int flush() {
    if(esObjectClient == null) {
      try {
        esObjectClient = new ESObjectClient<T>(new ESClient(connect), indexName, objectType) ;
        esObjectClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
        if(!esObjectClient.isCreated()) {
          String settingUrl = objectType.getName().replace('.', '/') + ".setting.json";
          String mappingUrl = objectType.getName().replace('.', '/') + ".mapping.json";
          
          String settingJson = IOUtil.getResourceAsString(settingUrl, "UTF-8");
          String mappingJson = IOUtil.getResourceAsString(mappingUrl, "UTF-8");
          esObjectClient.createIndexWith(settingJson, mappingJson);
        }
      } catch(Exception ex) {
        ex.printStackTrace();
        return 0;
      }
    }
    int count = 0;
    try {
      Segment<LogWithId<T>> segment = null ;
      while((segment = queue.nextReadSegment(1000)) != null) {
        segment.open();
        while(segment.hasNext()) {
          LogWithId<T> record = segment.nextObject() ;
          esObjectClient.put(record.getLog(), record.getId());
          count++;
        }
        segment.close();
        queue.commitReadSegment(segment);
      }
    } catch(ElasticsearchException ex) {
      ex.printStackTrace();
    } catch (InterruptedException e) {
    } catch(Exception ex) {
      ex.printStackTrace() ; 
    }
    return count ;
  }
  
  @SuppressWarnings("serial")
  static public class LogWithId<T> implements Serializable {
    private String id ;
    private T      log ;

    public LogWithId() {}
    
    public LogWithId(String id, T log) {
      this.id = id ;
      this.log = log ;
    }
    
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public T getLog() { return log; }
    public void setLog(T log) { this.log = log; }
  }
}
