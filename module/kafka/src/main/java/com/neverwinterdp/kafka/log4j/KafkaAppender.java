package com.neverwinterdp.kafka.log4j;

import java.io.IOException;

import org.apache.kafka.common.KafkaException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import com.neverwinterdp.buffer.chronicle.MultiSegmentQueue;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;

public class KafkaAppender extends AppenderSkeleton {
  private String   connects ;
  private String   topic ;
  private String   queueBufferDir;
  private int      queueMaxSizePerSegment = 100000;
  private boolean  queueError = false ;
  private MultiSegmentQueue<Log4jRecord> queue ; 
  
  private DeamonThread forwardThread ;
  
  public void close() {
    if(forwardThread != null) {
      forwardThread.exit = true ;
      forwardThread.interrupt() ; 
    }
  }

  public void activateOptions() {
    System.out.println("KafkaAppender: Start Activate Kafka log4j appender");
    try {
      queue = new MultiSegmentQueue<Log4jRecord>(queueBufferDir, queueMaxSizePerSegment) ;
    } catch (Exception e) {
      queueError = true ;
      e.printStackTrace();
    }
    forwardThread = new DeamonThread(); 
    forwardThread.setDaemon(true);
    forwardThread.start() ;
    System.out.println("KafkaAppender: Finish Activate Kafka log4j appender");
  }

  public void setConnects(String connects) { this.connects = connects ; }
  
  
  public void setTopic(String topic) {
    this.topic = topic ;
  }
 
  public void setQueueBufferDir(String queueBufferDir) { this.queueBufferDir = queueBufferDir; }

  public void setQueueMaxSizePerSegment(int queueMaxSizePerSegment) {
    this.queueMaxSizePerSegment = queueMaxSizePerSegment;
  }

  public boolean requiresLayout() { return false; }

  protected void append(LoggingEvent event) {
    if(queueError) return ;
    Log4jRecord record = new Log4jRecord(event) ;
    try {
      queue.writeObject(record) ;
    } catch (Exception e) {
      queueError = true ;
      e.printStackTrace();
    }
  }
  
  public class DeamonThread extends Thread {
    private KafkaWriter kafkaWriter;
    private boolean kafkaError = false ;
    private boolean exit = false ;
    
    boolean init() {
      try {
        kafkaWriter = new AckKafkaWriter("log4j", connects) ;
      } catch(Exception ex) {
        ex.printStackTrace();
        return false ;
      }
      return true ;
    }
    
    public void forward() {
      while(true) {
        try {
          if(kafkaError) {
            Thread.sleep(60 * 1000);
            kafkaWriter.reconnect();
            kafkaError = false ;
          }
          Segment<Log4jRecord> segment = null ;
          while((segment = queue.nextReadSegment(15000)) != null) {
            segment.open();
            while(segment.hasNext()) {
              Log4jRecord record = segment.nextObject() ;
              String json = JSONSerializer.INSTANCE.toString(record);
              kafkaWriter.send(topic, json, 60 * 1000);;
            }
            queue.commitReadSegment(segment);
          }
        } catch(KafkaException ex) {
          kafkaError = true ;
        } catch (InterruptedException e) {
          return ;
        } catch(Exception ex) {
          ex.printStackTrace() ; 
          return ;
        }
      }
    }
    
    void shutdown() {
      try {
        kafkaWriter.close() ;
      } catch (Exception e) {
        e.printStackTrace();
      }
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
