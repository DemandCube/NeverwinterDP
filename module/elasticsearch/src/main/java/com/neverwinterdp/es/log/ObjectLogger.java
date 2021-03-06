package com.neverwinterdp.es.log;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;

import com.neverwinterdp.buffer.chronicle.MultiSegmentQueue;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.text.StringUtil;

public class ObjectLogger<T> {
  private String[] esConnect ;
  private String   indexName ;
  private Class<T> objectType ;
  
  private MultiSegmentQueue<LogWithId<T>> queue ; 
  private boolean  queueError = false ;
  private ESObjectClient<T> esObjectClient ;
  
  public ObjectLogger(String[] esConnect, Class<T> objectType, String indexName, String queueBufferDir, int queueMaxSizePerSegment) throws Exception {
    this.esConnect    = esConnect;
    this.indexName  =  indexName;
    this.objectType = objectType;
    this.queue      = new MultiSegmentQueue<LogWithId<T>>(queueBufferDir, queueMaxSizePerSegment) ;
  }
  
  public ObjectLogger<T> setConnects(String connects) { 
    this.esConnect = StringUtil.toStringArray(connects) ; 
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
    if(esObjectClient != null) {
      esObjectClient.close();
    }
    queue.close(); 
  }
  
  synchronized public int flush() throws InterruptedException {
    if(esObjectClient == null) {
      try {
        esObjectClient = new ESObjectClient<T>(new ESClient(esConnect), indexName, objectType) ;
        esObjectClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
        synchronized(getClass()) {
          if(!esObjectClient.isCreated()) {
            String settingUrl = objectType.getName().replace('.', '/') + ".setting.json";
            String mappingUrl = objectType.getName().replace('.', '/') + ".mapping.json";
            String settingJson = IOUtil.getResourceAsString(settingUrl, "UTF-8");
            String mappingJson = IOUtil.getResourceAsString(mappingUrl, "UTF-8");
            esObjectClient.createIndexWith(settingJson, mappingJson);
          }
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
        Map<String, T> holder = new HashMap<String, T>();
        while(segment.hasNext()) {
          LogWithId<T> record = segment.nextObject() ;
          holder.put(record.getId(), record.getLog());
          if(holder.size() == 100) {
            esObjectClient.put(holder);
            holder.clear();
          }
          count++;
        }
        if(holder.size() > 0) {
          esObjectClient.put(holder);
        }
        segment.close();
        queue.commitReadSegment(segment);
      }
    } catch(ElasticsearchException ex) {
      ex.printStackTrace();
    } catch (InterruptedException e) {
      throw e;
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
