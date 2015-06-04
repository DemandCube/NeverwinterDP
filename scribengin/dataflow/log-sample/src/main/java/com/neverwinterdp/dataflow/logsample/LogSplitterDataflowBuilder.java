package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSource;
import com.neverwinterdp.util.JSONSerializer;

public class LogSplitterDataflowBuilder {
  private int numOfWorkers = 2;
  private int numOfExecutorPerWorker = 2;
  private ScribenginClient scribenginClient;
  
  public LogSplitterDataflowBuilder(ScribenginClient scribenginClient) {
    this.scribenginClient = scribenginClient;
  }

  
  public void setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
  }

  public void setNumOfExecutorPerWorker(int numOfExecutorPerWorker) {
    this.numOfExecutorPerWorker = numOfExecutorPerWorker;
  }

  public DataflowWaitingEventListener submit() throws Exception {
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("log-sample-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(LogSplitter.class.getName());
    
    StorageDescriptor source = 
        KafkaSource.createStorageDescriptor("LogSampleDataflow", "log4j", "127.0.0.1:2181", "raw");
    dflDescriptor.setSourceDescriptor(source);
    
    StorageDescriptor infoSink = 
        KafkaSource.createStorageDescriptor("LogSampleDataflow", "log4j.info", "127.0.0.1:2181", null);
    dflDescriptor.addSinkDescriptor("info", infoSink);
    
    StorageDescriptor warningSink = 
        KafkaSource.createStorageDescriptor("LogSampleDataflow", "log4j.warn", "127.0.0.1:2181", null);
    dflDescriptor.addSinkDescriptor("warn", warningSink);
    
    StorageDescriptor errorSink = 
        KafkaSource.createStorageDescriptor("LogSampleDataflow", "log4j.error", "127.0.0.1:2181", null);;
    dflDescriptor.addSinkDescriptor("error", errorSink);
    
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
    return scribenginClient.submit(dflDescriptor) ;
  }
}
