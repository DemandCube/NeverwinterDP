package com.neverwinterdp.scribengin.dataflow.example.hdfs;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.hdfs.HDFSStorage;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;
import com.neverwinterdp.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;

public class ExampleHdfsDataflowSubmitterTest {
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  int numMessages = 10000;
  String BASE_DIR = "build/working";
  Registry registry;
  
  /**
   * Setup a local Scribengin cluster
   * This sets up kafka, zk, and vm-master
   * @throws Exception
   */
  @Before
  public void setup() throws Exception{
    
    System.setProperty("app.home", BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    registry = RegistryConfig.getDefault().newInstance().connect();
    shell = localScribenginCluster.getShell();
    
  }
  
  /**
   * Destroy the local Scribengin cluster and clean up 
   * @throws Exception
   */
  @After
  public void teardown() throws Exception{
    localScribenginCluster.shutdown();
  }
  
  /**
   * Test our HDFS Dataflow Submitter
   * 1. Write data to Kafka into the input topic
   * 2. Run our dataflow
   * 3. Use a HDFS stream reader to read the data in the output HDFS partition and make sure its all present 
   * @throws Exception
   */
  @Test
  public void TestExampleSimpleDataflowSubmitterTest() throws Exception{
    //Create a new DataflowSubmitter with default properties
    ExampleHdfsDataflowSubmitter eds = new ExampleHdfsDataflowSubmitter(shell);
    
    //Populate kafka input topic with data
    sendKafkaData(localScribenginCluster.getKafkaCluster().getKafkaConnect(), eds.getInputTopic());
    
    //Submit the dataflow and wait for it to start running
    eds.submitDataflow(localScribenginCluster.getKafkaCluster().getZKConnect());
    //Output the registry for debugging purposes
    //shell.execute("registry dump");
    
    //Give the dataflow a second to get going
    Thread.sleep(10000);
    
  //Get basic info on the dataflow
    shell.execute("dataflow info --dataflow-id " + eds.getDataflowID());
    
    //Do some very simple verification to ensure our data has been moved correctly
    //We'll use some basic HDFS classes to do the reading, so we'll configure our local HDFS FS here
    Path path = new Path(eds.getHDFSLocation());
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    int numEntries = readDirsRecursive(fs, eds.getHDFSLocation());
    fs.close();
    
    //Make sure all the messages were written
    assertEquals(numEntries, numMessages);
    
    //Get basic info on the dataflow
    shell.execute("dataflow info --dataflow-id " + eds.getDataflowID());
  }
  
  /**
   * Use our HDFSSource to read our data through all partitions
   * @param fs HDFS File system
   * @param registryPath Path to HDFS info in the registry
   * @param hdfsPath Path our data is saved to
   * @return count of records in HDFS
   * @throws Exception
   */
  private int readDirsRecursive(FileSystem fs, String hdfsPath) throws Exception{
    int count = 0;
    
    //Configure our HDFS storage object
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("output", hdfsPath);
    HDFSStorage storage = new HDFSStorage(registry, fs, storageConfig);
    
    //Get our source object from the storage object
    HDFSSource source = storage.getSource();
    //Get all source streams
    SourcePartitionStream[] sourceStream = source.getLatestSourcePartition().getPartitionStreams();
    //Read from each individual source stream
    for(int i = 0; i < sourceStream.length; i++) {
      SourcePartitionStreamReader reader = sourceStream[i].getReader("reader-for-stream-" + i);
      Message message = null;
      //Count the number of messages
      while((message = reader.next(3000)) != null) {
        count++;
      }
      reader.close();
    }
    
    return count;
  }
  
  /**
   * Push data to Kafka
   * @param kafkaConnect Kafka's [host]:[port]
   * @param inputTopic Topic to write to
   */
  private void sendKafkaData(String kafkaConnect, String inputTopic){
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaConnect);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("request.required.acks", "1");
    props.put("retry.backoff.ms", "1000");
    ProducerConfig config = new ProducerConfig(props);
    
    Producer<String, String> producer = new Producer<String, String>(config);
    for(int i = 0; i < numMessages; i++) {
      String messageKey = "key-" + i;
      String message    = Integer.toString(i);
      producer.send(new KeyedMessage<String, String>(inputTopic, messageKey, message));
      if((i + 1) % 500 == 0) {
        System.out.println("Send " + (i + 1) + " messages");
      }
    }
    producer.close();
  }
  
}
