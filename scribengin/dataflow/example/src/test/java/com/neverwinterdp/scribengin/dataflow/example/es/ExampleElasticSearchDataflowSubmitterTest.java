package com.neverwinterdp.scribengin.dataflow.example.es;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.example.es.ExampleElasticSearchDataflowSubmitter.Config;
import com.neverwinterdp.scribengin.shell.ScribenginShell;

public class ExampleElasticSearchDataflowSubmitterTest {
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  int numMessages = 500;
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
   * Test our elasticsearch Dataflow Submitter
   * 1. Write data to Kafka into the input topic
   * 2. Run our dataflow
   * 3. Use a HDFS stream reader to read the data in the output HDFS partition and make sure its all present 
   * @throws Exception
   */
  @Test
  public void TestExampleElasticSearchDataflowSubmitter() throws Exception{
    Config c = new Config();
    
    ExampleElasticSearchDataflowSubmitter esd = new ExampleElasticSearchDataflowSubmitter(shell, c);
    sendKafkaData(localScribenginCluster.getKafkaCluster().getKafkaConnect(), esd.getInputTopic());
    esd.submitDataflow();
    
    //Give the dataflow a second to get going
    Thread.sleep(10000);
    
    //Get basic info on the dataflow
    shell.execute("dataflow info --dataflow-id " + esd.getDataflowID());
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
