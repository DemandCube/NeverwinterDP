package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSource;

public class LogPersisterDataflowBuilder {
  private String           sourceName;
  private int              numOfWorkers           = 2;
  private int              numOfExecutorPerWorker = 2;

  public LogPersisterDataflowBuilder(String sourceName) {
    this.sourceName = sourceName;
  }

  public DataflowDescriptor getDataflowDescriptor() {
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("info-log-persister-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(LogPerister.class.getName());
    
    StorageDescriptor source = 
        KafkaSource.createStorageDescriptor("LogSampleDataflow", sourceName, "127.0.0.1:2181", null);
    dflDescriptor.setSourceDescriptor(source);
    
    StorageDescriptor hdfsSink = new StorageDescriptor("HDFS",  "build/hdfs/info");
    dflDescriptor.addSinkDescriptor("hdfs", hdfsSink);
    
    return dflDescriptor ;
  }
}
