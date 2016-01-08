package com.neverwinterdp.scribengin.dataflow.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
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

public class ExampleDataflowSubmitterTest {
  
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  String inputTopic = "input.topic";
  String outputTopic = "output.topic";
  int numMessages = 10000;
  
  
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
  
  @After
  public void teardown() throws Exception{
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void TestExampleDataflowSubmitterTest() throws Exception{
    sendKafkaData(localScribenginCluster.getKafkaCluster().getKafkaConnect());
    
    
    ExampleDataflowSubmitter eds = new ExampleDataflowSubmitter(shell);
    eds.submitDataflow(localScribenginCluster.getKafkaCluster().getZKConnect());
    shell.execute("registry dump");
    
    ConsumerIterator<byte[], byte[]> it = getConsumerIterator(localScribenginCluster.getKafkaCluster().getZKConnect());
    
    int numReceived = 0;
    boolean[] assertionArray = new boolean[numMessages];
    
    //while(it.hasNext()){
    for(int i = 0; i < numMessages; i++){
      Message message = JSONSerializer.INSTANCE.fromBytes(it.next().message(), Message.class);
      String data = new String(message.getData());
      assertionArray[Integer.parseInt(data)] = true;
      //System.err.println(data);
      numReceived++;
    }
    
    assertEquals(numReceived, numMessages);
    for(boolean b: assertionArray){
      assertTrue(b);
    }
    
    shell.execute("dataflow info --dataflow-id ExampleDataflow");
    
  }
  
  
  private void sendKafkaData(String kafkaConnect){
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
  
  private ConsumerIterator<byte[], byte[]> getConsumerIterator(String zkConnect){
    Properties props = new Properties();
    props.put("zookeeper.connect", zkConnect);
    props.put("group.id", "default");
    
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(outputTopic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(outputTopic).get(0);
    return stream.iterator();
  }
  
}



