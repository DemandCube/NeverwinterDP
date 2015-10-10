package com.neverwinterdp.scribengin.storage.kafka.perftest;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.util.text.TabularFormater;

public class KafkaPerfTest {
  @Parameter(names = "--zk-connect", description = "Zk connect")
  private String zkConnect ;
  
  @Parameter(names = "--topic", description = "The topic")
  private String topic ;
  
  @Parameter(names = "--topic-num-of-message", description = "Num Of Message")
  private long topicNumOfMessages = 10000;
  
  @Parameter(names = "--topic-num-of-partition", description = "Topic Num Of Partition")
  private int topicNumOfPartitions = 10;
  
  @Parameter(names = "--topic-num-of-replication", description = "Topic Num Of Replication")
  private int topicNumOfReplications = 1;
  
  @Parameter(names = "--writer-write-per-writer", description = "max writer per a writer")
  private int writerWritePerWriter = 1000;
  
  @Parameter(names = "--reader-read-per-reader", description = "max read per a reader")
  private int readerReadPerReader = 1000;
  
  @Parameter(names = "--max-run-time", description = "Max Run Time")
  private long maxRunTime = 3 * 60000;
  
  
  public void run() throws Exception {
    TopicWriter topicWriter = new TopicWriter(zkConnect, topic, topicNumOfMessages);
    topicWriter.setNumOfPartitions(topicNumOfPartitions);
    topicWriter.setNumOfReplicatons(topicNumOfReplications);
    topicWriter.setWritePerWriter(writerWritePerWriter);
    topicWriter.setNumOfMessages(topicNumOfMessages);
    
    topicWriter.start();
    
    TopicReader topicReader = new TopicReader(topic, zkConnect);
    topicReader.setReadPerReader(readerReadPerReader);
    topicReader.start();
    
    topicWriter.waitForTermination(maxRunTime);
    topicReader.waitForTermination(10 * 60000);
    
    TabularFormater formater = new TabularFormater("Write", "Read");
    formater.setTitle("Topic Report");
    formater.addRow(topicWriter.getTotalWrite(), topicReader.getTotalRead());
    System.out.println(formater.getFormattedText());
  }
  
  static public void main(String[] args) throws Exception {
    KafkaPerfTest test = new KafkaPerfTest();
    new JCommander(test, args);
    test.run();
  }
}
