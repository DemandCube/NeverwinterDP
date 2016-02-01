package com.neverwinterdp.scribengin.dataflow.example.simple;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;

public class ExampleSimpleDataflowSubmitterTest {
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  int numMessages = 10000;
  
  /**
   * Setup a local Scribengin cluster
   * This sets up kafka, zk, and vm-master
   * @throws Exception
   */
  @Before
  public void setup() throws Exception{
    String BASE_DIR = "build/working";
    System.setProperty("app.home", BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    
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
   * Test our Simple Dataflow Submitter
   * 1. Write data to Kafka into the input topic
   * 2. Run our dataflow
   * 3. Use a Kafka Consumer to read the data in the output topic and make sure its all present 
   * @throws Exception
   */
  @Test
  public void testExampleSimpleDataflowSubmitterTest() throws Exception{
    //Create a new DataflowSubmitter with default properties
    ExampleSimpleDataflowSubmitter eds = new ExampleSimpleDataflowSubmitter(shell);
    
    //Populate kafka input topic with data
    sendKafkaData(localScribenginCluster.getKafkaCluster().getKafkaConnect(), eds.getInputTopic());
    
    //Submit the dataflow and wait for it to start running
    eds.submitDataflow(localScribenginCluster.getKafkaCluster().getZKConnect());
    //Output the registry for debugging purposes
    shell.execute("registry dump");
    
    //Get basic info on the dataflow
    shell.execute("dataflow info --dataflow-id "+eds.getDataflowID());
    
    //Get the kafka output topic iterator
    ConsumerIterator<byte[], byte[]> it = 
        getConsumerIterator(localScribenginCluster.getKafkaCluster().getZKConnect(), eds.getOutputTopic());
    
    //Do some very simple verification to ensure our data has been moved correctly
    int numReceived = 0;
    int[] assertionArray = new int[numMessages];
    Arrays.fill(assertionArray, 0);
    
    try{
      while(it.hasNext()){
        //This is how we serialize our data back into our Messsage object
        Message message = JSONSerializer.INSTANCE.fromBytes(it.next().message(), Message.class);
        String data = new String(message.getData());
        assertionArray[Integer.parseInt(data)]++;
        //System.err.println(data);
        numReceived++;
      }
    } catch (ConsumerTimeoutException e){}
    
    assertEquals(numReceived, numMessages);
    for(int b: assertionArray){
      assertEquals(1, b);
    }
    
    //Get basic info on the dataflow
    shell.execute("dataflow info --dataflow-id " + eds.getDataflowID());
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
    ProducerConfig config = new ProducerConfig(props);
    
    Producer<String, String> producer = new Producer<String, String>(config);
    for(int i = 0; i < numMessages; i++){
      producer.send(new KeyedMessage<String, String>(inputTopic, "test", Integer.toString(i)));
    }
    producer.close();
  }
  
  private ConsumerIterator<byte[], byte[]> getConsumerIterator(String zkConnect, String topic){
    Properties props = new Properties();
    props.put("zookeeper.connect", zkConnect);
    props.put("group.id", "default");
    props.put("consumer.timeout.ms", "5000");
    
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
    return stream.iterator();
  }
}