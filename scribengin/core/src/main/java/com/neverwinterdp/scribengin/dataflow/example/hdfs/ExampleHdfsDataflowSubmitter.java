package com.neverwinterdp.scribengin.dataflow.example.hdfs;

import java.util.Properties;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.dataflow.example.simple.SimpleDataStreamOperator;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;

public class ExampleHdfsDataflowSubmitter {
  private String dataflowID;
  private int defaultReplication;
  private int defaultParallelism;
  
  private int numOfWorker;
  private int numOfExecutorPerWorker;
  
  private String inputTopic;
  
  private String HDFSRegistryPath;
  private String HDFSLocation;
  
  private ScribenginShell shell;
  private DataflowSubmitter submitter;
  
  public ExampleHdfsDataflowSubmitter(ScribenginShell shell){
    this(shell, new Properties());
  }
  
  /**
   * Constructor - sets shell to access Scribengin and configuration properties 
   * @param shell ScribenginShell to connect to Scribengin with
   * @param props Properties to configure the dataflow
   */
  public ExampleHdfsDataflowSubmitter(ScribenginShell shell, Properties props){
    //This it the shell to communicate with Scribengin with
    this.shell = shell;
    
    //The dataflow's ID.  All dataflows require a unique ID when running
    dataflowID = props.getProperty("dataflow.id", "ExampleDataflow");
    
    //The default replication factor for Kafka
    defaultReplication = Integer.parseInt(props.getProperty("dataflow.replication", "1"));
    //The number of DataStreams to deploy 
    defaultParallelism = Integer.parseInt(props.getProperty("dataflow.parallelism", "8"));
    
    //The number of workers to deploy (i.e. YARN containers)
    numOfWorker                = Integer.parseInt(props.getProperty("dataflow.numWorker", "2"));
    //The number of executors per worker (i.e. threads per YARN container)
    numOfExecutorPerWorker     = Integer.parseInt(props.getProperty("dataflow.numExecutorPerWorker", "2"));
    
    //The kafka input topic
    inputTopic = props.getProperty("dataflow.inputTopic", "input.topic");
    
    //Where in the registry to store HDFS info (Not suggested to change this)
    HDFSRegistryPath = props.getProperty("dataflow.hdfsRegistryPath", "/storage/hdfs/output");
    
    //Where in HDFS to store our data
    HDFSLocation = props.getProperty("dataflow.hdfsLocation", "build/working/storage/hdfs/output");
    
  }
  
  /**
   * The logic to submit the dataflow
   * @param kafkaZkConnect [host]:[port] of Kafka's Zookeeper conenction 
   * @throws Exception
   */
  public void submitDataflow(String kafkaZkConnect) throws Exception{
    Dataflow<Message, Message> dfl = buildDataflow(kafkaZkConnect);
    //Get the dataflow's descriptor
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    //Output the descriptor in human-readable JSON
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));

    //Ensure all your sources and sinks are up and running first, then...

    //Submit the dataflow and wait until it starts running
    submitter = new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForDataflowRunning(60000);
    
  }
  
  /**
   * Wait for the dataflow to complete within the given timeout
   * @param timeout Timeout in ms
   * @throws Exception
   */
  public void waitForDataflowCompletion(int timeout) throws Exception{
    this.submitter.waitForDataflowStop(timeout);
  }
  
  /**
   * The logic to build the dataflow configuration
   * @param kafkaZkConnect [host]:[port] of Kafka's Zookeeper conenction 
   * @return
   */
  public Dataflow<Message,Message> buildDataflow(String kafkaZkConnect){
    //Create the new Dataflow object
    // <Message,Message> pertains to the <input,output> object for the data
    Dataflow<Message,Message> dfl = new Dataflow<Message,Message>(dataflowID);
    dfl.
      setDefaultParallelism(defaultParallelism).
      setDefaultReplication(defaultReplication);
    
    dfl.getWorkerDescriptor().setNumOfInstances(numOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(numOfExecutorPerWorker);
    
    
    //Define our input source - set name, ZK host:port, and input topic name
    KafkaDataSet<Message> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", kafkaZkConnect, inputTopic));
    
    //Our output sink will be HDFS
    //"output" - the dataset's name
    //HDFSRegistryPath - where in the registry to HDFS config
    //HDFSLocation - the path in HDFS to put our data
    DataSet<Message> outputDs = dfl.createOutput(new HDFSStorageConfig("output", HDFSRegistryPath, HDFSLocation));
    
    //Define which operator to use.  
    //This will be the logic that ties the input to the output
    Operator<Message, Message> operator     = dfl.createOperator("simpleOperator", SimpleDataStreamOperator.class);
    
    //Connect your input to the operator
    inputDs.useRawReader().connect(operator);
    //Connect your operator to the output
    operator.connect(outputDs);

    return dfl;
  }
  
  
  public String getDataflowID() {
    return dataflowID;
  }

  public void setDataflowID(String dataflowID) {
    this.dataflowID = dataflowID;
  }

  public int getDefaultReplication() {
    return defaultReplication;
  }

  public void setDefaultReplication(int defaultReplication) {
    this.defaultReplication = defaultReplication;
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public void setDefaultParallelism(int defaultParallelism) {
    this.defaultParallelism = defaultParallelism;
  }


  public int getNumOfWorker() {
    return numOfWorker;
  }

  public void setNumOfWorker(int numOfWorker) {
    this.numOfWorker = numOfWorker;
  }

  public int getNumOfExecutorPerWorker() {
    return numOfExecutorPerWorker;
  }

  public void setNumOfExecutorPerWorker(int numOfExecutorPerWorker) {
    this.numOfExecutorPerWorker = numOfExecutorPerWorker;
  }

  public String getInputTopic() {
    return inputTopic;
  }

  public void setInputTopic(String inputTopic) {
    this.inputTopic = inputTopic;
  }

  public ScribenginShell getShell() {
    return shell;
  }

  public void setShell(ScribenginShell shell) {
    this.shell = shell;
  }

  public DataflowSubmitter getSubmitter() {
    return submitter;
  }

  public void setSubmitter(DataflowSubmitter submitter) {
    this.submitter = submitter;
  }
  
  public String getHDFSRegistryPath() {
    return HDFSRegistryPath;
  }

  public void setHDFSRegistryPath(String hDFSRegistryPath) {
    HDFSRegistryPath = hDFSRegistryPath;
  }

  public String getHDFSLocation() {
    return HDFSLocation;
  }

  public void setHDFSLocation(String hDFSLocation) {
    HDFSLocation = hDFSLocation;
  }

}
