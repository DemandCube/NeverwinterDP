package com.neverwinterdp.storage.kafka.perftest;

import com.beust.jcommander.Parameter;

public class TopicPerfConfig implements Cloneable {
  @Parameter(names = "--zk-connect", description = "Zk connect")
  public String zkConnect ;
  
  @Parameter(names = "--topic", description = "The topic")
  public String topic ;
  
  @Parameter(names = "--topic-num-of-message", description = "Num Of Message")
  public long topicNumOfMessages = 10000;
  
  @Parameter(names = "--topic-num-of-partition", description = "Topic Num Of Partition")
  public int topicNumOfPartitions = 10;
  
  @Parameter(names = "--topic-num-of-replication", description = "Topic Num Of Replication")
  public int topicNumOfReplications = 1;
  
  @Parameter(names = "--writer-write-per-writer", description = "max writer per a writer")
  public int writerWritePerWriter = 1000;
  
  @Parameter(names = "--writer-write-break-in-period", description = "write break-in period to slow down")
  public long writerWriteBreakInPeriod = 0;
  
  @Parameter(names = "--reader-run-delay", description = "max read per a reader")
  public long readerRunDelay = 5000;
  
  @Parameter(names = "--reader-read-per-reader", description = "max read per a reader")
  public int readerReadPerReader = 1000;
  
  @Parameter(names = "--max-runtime", description = "Max Run Time")
  public long maxRunTime = 3 * 60000;

  public TopicPerfConfig clone() throws CloneNotSupportedException {
    return (TopicPerfConfig) super.clone();
  }
}
