package com.neverwinterdp.es.log4j;

import java.io.IOException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.ElasticsearchException;

import com.neverwinterdp.buffer.chronicle.MultiSegmentQueue;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.util.text.StringUtil;

public class ElasticSearchAppender extends AppenderSkeleton {
  private String[] connect ;
  private String   indexName ;
  private String   queueBufferDir;
  private int      queueMaxSizePerSegment = 100000;
  private String   appHost = null;
  private String   appName  = null; 
  
  private MultiSegmentQueue<Log4jRecord> queue ; 
  private boolean  queueError = false ;
  
  private DeamonThread forwardThread ;

  public void setConnects(String connects) {
    this.connect = StringUtil.toStringArray(connects) ;
  }
  
  public void setIndexName(String indexName) {
    this.indexName = indexName ;
  }
 
  public void setQueueBufferDir(String queueBufferDir) { 
    this.queueBufferDir = queueBufferDir; 
  }

  public void setQueueMaxSizePerSegment(int queueMaxSizePerSegment) {
    this.queueMaxSizePerSegment = queueMaxSizePerSegment;
  }
  
  public void setAppHost(String appHost) { this.appHost = appHost; }

  public void setAppName(String appName) { this.appName = appName; }

  public void init(String[] connect, String indexName, String queueBufferDir) {
    this.connect = connect;
    this.indexName = indexName;
    this.queueBufferDir = queueBufferDir;
  }
  
  public void close() {
    if(forwardThread != null) {
      forwardThread.exit = true ;
      forwardThread.interrupt() ; 
    }
  }
  
  public void activateOptions() {
    try {
      queue = new MultiSegmentQueue<Log4jRecord>(queueBufferDir, queueMaxSizePerSegment) ;
    } catch (Exception e) {
      queueError = true ;
      e.printStackTrace();
    }
    if(appHost == null) appHost = System.getProperty("log4j.app.host");
    if(appName == null) appName = System.getProperty("log4j.app.name");
    forwardThread = new DeamonThread(); 
    forwardThread.start() ;
  }
  
  public boolean requiresLayout() { return false; }

  protected void append(LoggingEvent event) {
    if(queueError) return ;
    Log4jRecord record = new Log4jRecord(event) ;
    record.setHost(appHost);
    record.setAppName(appName);
    try {
      queue.writeObject(record) ;
    } catch(Throwable e) {
      queueError = true ;
      e.printStackTrace();
    }
  }
  
  public class DeamonThread extends Thread {
    private ESObjectClient<Log4jRecord> esLog4jRecordClient ;
    private boolean elasticsearchError = false ;
    private boolean exit = false ;
    
    boolean init() {
      try {
        esLog4jRecordClient = new ESObjectClient<Log4jRecord>(new ESClient(connect), indexName, Log4jRecord.class) ;
        esLog4jRecordClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
        if(!esLog4jRecordClient.isCreated()) {
          String settingUrl = Log4jRecord.class.getName().replace('.', '/') + ".setting.json";
          String mappingUrl = Log4jRecord.class.getName().replace('.', '/') + ".mapping.json";
          
          String settingJson = IOUtil.getResourceAsString(settingUrl, "UTF-8");
          String mappingJson = IOUtil.getResourceAsString(mappingUrl, "UTF-8");
          esLog4jRecordClient.createIndexWith(settingJson, mappingJson);
        }
      } catch(Exception ex) {
        ex.printStackTrace();
        return false ;
      }
      return true ;
    }
    
    public void forward() {
      while(true) {
        try {
          if(elasticsearchError) {
            Thread.sleep(60 * 1000);
            elasticsearchError = false ;
          }
          while(true) {
            Segment<Log4jRecord> segment = queue.nextReadSegment(5000);
            if(segment == null) continue;
            segment.open();
            while(segment.hasNext()) {
              Log4jRecord record = segment.nextObject() ;
              esLog4jRecordClient.put(record, record.getId());
            }
            queue.commitReadSegment(segment);
          }
        } catch(ElasticsearchException ex) {
          elasticsearchError = true ;
        } catch (InterruptedException e) {
          return ;
        } catch(Throwable ex) {
          ex.printStackTrace() ; 
          return ;
        }
      }
    }
    
    void shutdown() {
      esLog4jRecordClient.close() ;
      if(exit) {
        try {
          if(queue != null) queue.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
    public void run() {
      if(!init()) return ;
      forward() ;
      shutdown() ;
    }
  }
}
