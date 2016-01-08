package com.neverwinterdp.scribengin.dataflow.example;

import java.util.Properties;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;

public class ExampleDataflowSubmitter {
  private String dataflowID;
  private int defaultReplication;
  private int defaultParallelism;
  private int maxRunTime;
  private int windowSize;
  private int slidingWindowSize;
  
  private int numOfWorker;
  private int numOfExecutorPerWorker;
  
  private String inputTopic;
  private String outputTopic;
  
  private ScribenginShell shell;
  private DataflowSubmitter submitter;
  
  public ExampleDataflowSubmitter(ScribenginShell shell, Properties props){
    this.shell = shell;
    
    dataflowID = props.getProperty("dataflow.id", "ExampleDataflow");
    defaultReplication = Integer.parseInt(props.getProperty("dataflow.replication", "1"));
    defaultParallelism = Integer.parseInt(props.getProperty("dataflow.parallelism", "8"));
    maxRunTime         = Integer.parseInt(props.getProperty("dataflow.maxRunTime", "10000"));
    windowSize         = Integer.parseInt(props.getProperty("dataflow.windowSize", "1000"));
    slidingWindowSize  = Integer.parseInt(props.getProperty("dataflow.slidingWindowSize", "500"));
    
    numOfWorker                = Integer.parseInt(props.getProperty("dataflow.numWorker", "4"));
    numOfExecutorPerWorker     = Integer.parseInt(props.getProperty("dataflow.numExecutorPerWorker", "2"));
    
    inputTopic = props.getProperty("dataflow.inputTopic", "input.topic");
    outputTopic = props.getProperty("dataflow.outputTopic", "output.topic");
    
  }
  
  public void submitDataflow(String kafkaZkConnect) throws Exception{
    Dataflow<Message, Message> dfl = buildDataflow(kafkaZkConnect);
    //Get the dataflow's descriptor
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    //Output the descriptor in human-readable JSON
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));

    //Ensure all your sources and sinks are up and running first, then...

    //Submit the dataflow and wait until it starts running
    submitter = new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForRunning(60000);
    
  }
  
  public void waitForDataflowCompletion(int timeout) throws Exception{
    this.submitter.waitForFinish(timeout);
  }
  
  public Dataflow<Message,Message> buildDataflow(String kafkaZkConnect){
    Dataflow<Message,Message> dfl = new Dataflow<Message,Message>(dataflowID);
    dfl.
      setDefaultParallelism(defaultParallelism).
      setDefaultReplication(defaultReplication).
      setMaxRuntime(maxRunTime).
      setTrackingWindowSize(windowSize).
      setSlidingWindowSize(slidingWindowSize);
    
    dfl.getWorkerDescriptor().setNumOfInstances(numOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(numOfExecutorPerWorker);
    
    
    //Define our input source - set name, ZK host:port, and input topic name
    KafkaDataSet<Message> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", kafkaZkConnect, inputTopic));
    
    //Define our output sink - set name, ZK host:port, and output topic name
    DataSet<Message> outputDs = 
        dfl.createOutput(new KafkaStorageConfig("output", kafkaZkConnect, outputTopic));
    
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

  public int getMaxRunTime() {
    return maxRunTime;
  }

  public void setMaxRunTime(int maxRunTime) {
    this.maxRunTime = maxRunTime;
  }

  public int getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(int windowSize) {
    this.windowSize = windowSize;
  }

  public int getSlidingWindowSize() {
    return slidingWindowSize;
  }

  public void setSlidingWindowSize(int slidingWindowSize) {
    this.slidingWindowSize = slidingWindowSize;
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

  public String getOutputTopic() {
    return outputTopic;
  }

  public void setOutputTopic(String outputTopic) {
    this.outputTopic = outputTopic;
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
}
