**Table of Contents**  

- [Introduction](#introduction)
- [Sample Code](#sample-code)
- [A Simple Dataflow](#a-simple-dataflow)
  - [Overview](#overview)
  - [Creating a Simple DataStreamOperator](#creating-a-simple-dataStreamOperator)
  - [Create a Simple DataflowSubmitter](#create-a-simple-dataflowSubmitter)
  - [Create a Unit Test](#create-a-unit-test)
  - [Launching in a real Scribengin Cluster](#launching-in-a-real-scribengin-cluster)

#Introduction#

This howto will show you how to develop your own dataflow and operator.  You will then learn how to submit your dataflow to Scribengin to be run.  

#Sample code#
You can find sample code in the Scribengin package com.neverwinterdp.scribengin.dataflow.example.*. The code comes complete with unit tests and full comments.

#A Simple Dataflow


##Overview##
In order to create a dataflow you need to prepare your sources and sinks, have data in the source, understand your data structure, and decide how to transform the data.

![Simple Dataflow Example](../images/API_Example_Simple.png "Simple Dataflow Example")

- Create a DataStreamOperator
  - Dataflows split the movement of data into streams for better performance and concurrency
  - An Operator handles moving data from an input to an output
  - An Operator is made up of multiple DataStreamOperators.  Each DataStreamOperator handles a stream of data.
  - A DataStreamOperator can have any arbitrary logic to move, filter, or enhance the data.
- Create a DataflowSubmitter
  -  Once your DataStreamOperator is defined, we'll need to define what our dataflow looks like.
  -  We need to define what the source of data is, which DataStreamOperator to use, and where the data ends up at
- Create a unit test
  - Its much easier to run testing locally before you try deploying to a cluster
- Launch for real!   

##Creating a Simple DataStreamOperator

Our simple DataStreamOperator only takes in the record, and immediately writes it back out to any and all sinks.

```java
package com.neverwinterdp.scribengin.dataflow.example.simple;

import java.util.Set;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;

//You must extend DataStreamOperator
public class SimpleDataStreamOperator extends DataStreamOperator{

  //You must override the process() method
  @Override
  public void process(DataStreamOperatorContext ctx, Message record)
      throws Exception {
    
    //Get all sinks
    Set<String> sink = ctx.getAvailableOutputs();
    //For each sink, write the record
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
  }

}
```

##Create a Simple DataflowSubmitter

Our DataflowSubmitter is where we configure out Dataflow.  We need configure things like how many workers Scribengin will allocate, where our sources and sinks are, and which DataStreamOperator to use.

Our simple dataflow will do some extra things that are only for the sake of an example.  This program's main, when launched, will:
- Configure the dataflow
- Fill a Kafka Topic with data
- Launch our dataflow
- Then validate all the data made it through successfully

A real dataflow won't be producing its own data for it to consume, but for the sake of succinctness, we've included it here.

```java
package com.neverwinterdp.scribengin.dataflow.example.simple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleDataflowExample {
  /**
   * Simple class to house our configuration options for JCommander 
   */
  static public class Config {
    @Parameter(names = {"--help", "-h"}, help = true, description = "Output this help message")
    private boolean help;

    @Parameter(names = "--local-app-home", required=true, description="The example dataflow local location")
    String localAppHome ;
    
    @Parameter(names = "--dfs-app-home", description="DFS location to upload the example dataflow")
    String dfsAppHome = "/applications/dataflow/example";
    
    @Parameter(names = "--zk-connect", description="[host]:[port] of Zookeeper server")
    String zkConnect = "zookeeper-1:2181";
    
    @Parameter(names = "--hadoop-master-connect", description="Hostname of HadoopMaster")
    String hadoopMasterConnect = "hadoop-master";

    @Parameter(names = "--dataflow-id", description = "Unique ID for the dataflow")
    String dataflowId        = "ExampleDataflow";
    
    @Parameter(names = "--dataflow-default-replication", description = "Dataflow default replication")
    int dataflowDefaultReplication = 1;
    
    @Parameter(names = "--dataflow-default-parallelism", description = "The dataflow default parallelism")
    int dataflowDefaultParallelism = 8;
    
    @Parameter(names = "--dataflow-num-of-worker", description = "Number of workers to request")
    int dataflowNumOfWorker = 2;
    
    @Parameter(names = "--dataflow-num-of-executor-per-worker", description = "Number of Executors per worker to request")
    int dataflowNumOfExecutorPerWorker = 2;
    
    @Parameter(names = "--input-topic", description = "Name of input Kafka Topic")
    String inputTopic = "input.topic";
    
    @Parameter(names = "--input-num-of-messages", description = "Name of input Kafka Topic")
    int inputNumOfMessages = 10000;
    
    @Parameter(names = "--output-topic", description = "Name of output Kafka topic")
    String outputTopic ="output.topic";
  }
  
  private Config config = new Config();
  
  private ScribenginShell shell;
  
  /**
   * Constructor - sets shell to access Scribengin and configuration properties 
   * @param shell ScribenginShell to connect to Scribengin with
   * @param props Properties to configure the dataflow
   */
  public SimpleDataflowExample(ScribenginShell shell, Config config){
    this.shell = shell;
    this.config = config;
  }
  
  /**
   * The logic to submit the dataflow
   * @throws Exception
   */
  public void submitDataflow() throws Exception {
    //Upload our app to HDFS
    VMClient vmClient = shell.getScribenginClient().getVMClient();
    vmClient.uploadApp(config.localAppHome, config.dfsAppHome);
    
    Dataflow<Message, Message> dfl = buildDataflow();
    //Get the dataflow's descriptor
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    //Output the descriptor in human-readable JSON
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));

    //Ensure all your sources and sinks are up and running first, then...

    //Submit the dataflow and wait until it starts running
    DataflowSubmitter submitter = new DataflowSubmitter(shell.getScribenginClient(), dfl);
    submitter.submit().waitForDataflowRunning(60000);

    /** Wait for the dataflow to complete within the given timeout */
    //submitter.waitForDataflowStop(60000);
  }
  
  /**
   * The logic to build the dataflow configuration
   * @param kafkaZkConnect [host]:[port] of Kafka's Zookeeper conenction 
   * @return
   */
  public Dataflow<Message,Message> buildDataflow(){
    //Create the new Dataflow object
    // <Message,Message> pertains to the <input,output> object for the data
    Dataflow<Message,Message> dfl = new Dataflow<Message,Message>(config.dataflowId);
    dfl.
      setDFSAppHome(config.dfsAppHome).
      setDefaultParallelism(config.dataflowDefaultParallelism).
      setDefaultReplication(config.dataflowDefaultReplication);
    
    dfl.getWorkerDescriptor().setNumOfInstances(config.dataflowNumOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(config.dataflowNumOfExecutorPerWorker);
    
    
    //Define our input source - set name, ZK host:port, and input topic name
    KafkaDataSet<Message> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", config.zkConnect, config.inputTopic));
    
    //Define our output sink - set name, ZK host:port, and output topic name
    DataSet<Message> outputDs = 
        dfl.createOutput(new KafkaStorageConfig("output", config.zkConnect, config.outputTopic));
    
    //Define which operator to use.  
    //This will be the logic that ties the input to the output
    Operator<Message, Message> operator     = dfl.createOperator("simpleOperator", SimpleDataStreamOperator.class);
    
    //Connect your input to the operator
    inputDs.useRawReader().connect(operator);
    //Connect your operator to the output
    operator.connect(outputDs);

    return dfl;
  }
  
  /**
   * Push data to Kafka
   * @param kafkaConnect Kafka's [host]:[port]
   * @param inputTopic Topic to write to
   * @throws Exception 
   */
  public void createInputMessages() throws Exception {
    KafkaClient kafkaTool = new KafkaClient("KafkaTool", config.zkConnect);
    String kafkaBrokerConnects = kafkaTool.getKafkaBrokerList();
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaBrokerConnects);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("request.required.acks", "1");
    props.put("retry.backoff.ms", "1000");
    ProducerConfig producerConfig = new ProducerConfig(props);
    
    Producer<String, String> producer = new Producer<String, String>(producerConfig);
    for(int i = 0; i < config.inputNumOfMessages; i++){
      String messageKey = "key-" + i;
      String message    = Integer.toString(i);
      producer.send(new KeyedMessage<String, String>(config.inputTopic, messageKey, message));
      if((i + 1) % 500 == 0) {
        System.out.println("Send " + (i + 1) + " messages");
      }
    }
    producer.close();
  }
  
  /**
   * Reads in from a Kafka topic and ensures all messages have been consumed
   * @return True if test passes, False otherwise
   * @throws Exception
   */
  public boolean validate() throws Exception {
    ConsumerIterator<byte[], byte[]> it = getConsumerIterator(config.zkConnect, config.outputTopic);
    int[] output = new int[config.inputNumOfMessages];
    Arrays.fill(output, -1);
    int count = 0;
    try {
      while(it.hasNext()) {
        Message message = JSONSerializer.INSTANCE.fromBytes(it.next().message(), Message.class);
        String data = new String(message.getData());
        int value = Integer.parseInt(data);
        output[value] = value;
        count++ ;
        if(count % 500 == 0) {
          System.out.println("Read " + count + " messages");
        }
      }
    } catch (ConsumerTimeoutException e) { 
     //e.printStackTrace();
    }
    
    if(count != config.inputNumOfMessages) {
      throw new Exception("Input " + config.inputNumOfMessages + ", but can read only " + config.inputNumOfMessages + " messages");
    }
    
    for(int i = 0; i < output.length; i++) {
      if(i != output[i]) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Get Kafka Consumer for topic
   * @param zkConnect Zookeeper [host]:[port]
   * @param topic Topic to read from
   * @return
   */
  private ConsumerIterator<byte[], byte[]> getConsumerIterator(String zkConnect, String topic){
    Properties props = new Properties();
    props.put("zookeeper.connect", zkConnect);
    props.put("group.id", "default");
    props.put("consumer.timeout.ms", "5000");
    props.put("auto.offset.reset", "smallest");
    
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
    return stream.iterator();
  }
  
  static public void main(String args[]) throws Exception {
    //Use JCommander to parse command line args
    Config config = new Config();
    JCommander jCommander = new JCommander(config, args);
    
    if (config.help) {
      jCommander.usage();
      return;
    }
    
    //Create a registry configuration and point it to our running Registry (Zookeeper)
    RegistryConfig registryConfig = RegistryConfig.getDefault();
    registryConfig.setConnect(config.zkConnect);
    Registry registry = null;
    try{
      registry = new RegistryImpl(registryConfig).connect();
    } catch(Exception e){
      System.err.println("Could not connect to the registry at: "+ registryConfig.getConnect()+"\n"+e.getMessage());
      return;
    }
    
    //Configure where our hadoop master lives
    String hadoopMaster = config.hadoopMasterConnect;
    HadoopProperties hadoopProps = new HadoopProperties() ;
    hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
    hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
    
    //Set up our connection to Scribengin
    YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    ScribenginShell shell = new ScribenginShell(vmClient) ;
    shell.attribute(HadoopProperties.class, hadoopProps);
    
    SimpleDataflowExample simpleDataflowExample = new SimpleDataflowExample(shell, config);
    //Create input data for our dataflow to consume
    simpleDataflowExample.createInputMessages();
    //Launch our configured dataflow
    simpleDataflowExample.submitDataflow();
    
    //Wait to make sure that dataflow is running and produce some messages to the output topic
    Thread.sleep(1500);
    //Validate all messages have been consumed
    simpleDataflowExample.validate();
    
    //Get some info on the running dataflow
    shell.execute("dataflow info --dataflow-id " + config.dataflowId);
    
    //Close connection with Scribengin
    shell.close();
    shell.console().println("Simple Example Datafow is done!!!");
    System.exit(0);
  }
}


```


##Create a Unit Test 

Before we launch in a real cluster, we want to clear our code of any bugs before we move forward.  Let's go through some of the techniques for writing a unit test.

Our basic methodolgy for testing will be:

1. Setup an in-memory Scribengin cluster
2. Write data to Kafka
3. Launch our dataflow
4. Ensure the data has been moved to our output sink

```java
package com.neverwinterdp.scribengin.dataflow.example.simple;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.example.simple.SimpleDataflowExample.Config;
import com.neverwinterdp.scribengin.shell.ScribenginShell;

public class SimpleDataflowExampleUnitTest {
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  int numMessages = 10000;
  
  /**
   * Setup a local Scribengin cluster. This sets up kafka, zk, and vm-master
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
    Thread.sleep(5000);
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
  public void testExampleSimpleDataflow() throws Exception{
    //Create a new DataflowSubmitter with default properties
    String[] args = {
        //Since we're testing locally, we don't need to upload our app to DFS
        "--local-app-home", "N/A", 
        "--zk-connect", localScribenginCluster.getKafkaCluster().getZKConnect()
     };
    Config config = new Config();
    new JCommander(config, args);
    SimpleDataflowExample simpleDataflowExample = new SimpleDataflowExample(shell, config);
    simpleDataflowExample.createInputMessages();
    
    //Submit the dataflow and wait for it to start running
    simpleDataflowExample.submitDataflow();
    Thread.sleep(15000);
    //Output the registry for debugging purposes, shell.execute("registry dump");
    
    //Get basic info on the dataflow
    shell.execute("dataflow info --dataflow-id " + config.dataflowId);
    
    Assert.assertTrue(simpleDataflowExample.validate());
  }
}

```

##Launching in a real Scribengin Cluster

Launching in a real cluster will take one last step.  Below, we'll see the code required to push our dataflow configuration to Scribengin.  We'll need a few key pieces of information - You'll need to specify the [host]:[port] of Scribengin's Registry (Zookeeper) as well as any dataflow configuration.  The following example sets defaults, but uses JCommander to pass options on the command line.

![Scribengin Dataflow Submission Design](../images/dataflowsubmission.png "Scribengin Dataflow Submission Design")



###Building

We'll build our dataflow and Scribengin

```
cd NeverwinterDP
./gradlew clean build install release -x test

#A script will be output to 
NeverwinterDP/release/build/release/dataflow/example/bin/run-simple.sh
```


###Deploying

[Follow the instructions on how to start Scribengin here](../deployment/scribengin-cluster-setup-quickstart.md#arbitrary-cluster-setup)

###Running

Once Scribengin is up and running in YARN, we'll use the script created during the build to execute.  The script looks like this:

```
#!/bin/bash

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

#Get all our info about Java and our environment
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
APP_DIR=`cd $bin/..; pwd; cd $bin`
SCRIBENGIN_APP_DIR="$APP_DIR/../../scribengin"

#Set Hadoop user name
export HADOOP_USER_NAME="neverwinterdp"

#This is our script's directory
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#Get Java executable info and Java_Opts
JAVACMD=$JAVA_HOME/bin/java
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx512m -XX:-UseSplitVerifier" 

#This is the class we wrote
MAIN_CLASS="com.neverwinterdp.scribengin.dataflow.example.simple.SimpleDataflowExample"

#Spit out what we're about to run
echo "Command: $JAVACMD -Djava.ext.dirs=$SCRIBENGIN_APP_DIR/libs:$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $MAIN_CLASS --local-app-home $APP_DIR $@"

#Run!
$JAVACMD -Djava.ext.dirs=$SCRIBENGIN_APP_DIR/libs:$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $MAIN_CLASS --local-app-home $APP_DIR "$@"

```




