package com.neverwinterdp.scribengin.dataflow.example;

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
  private String dataflowID = "ExampleDataflow";
  private int defaultReplication = 1;
  private int defaultParallelism = 8;
  private int maxRunTime         = 10000;
  private int windowSize         = 1000;
  private int slidingWindowSize  = 500;
  
  private int numOfWorker                = 4;
  private int numOfExecutorPerWorker     = 2;
  
  private String inputTopic = "input.topic";
  private String outputTopic = "output.topic";
  
  private ScribenginShell shell;
  
  public ExampleDataflowSubmitter(ScribenginShell shell){
    this.shell = shell;
  }
  
  public void submitDataflow(String kafkaZkConnect) throws Exception{
    Dataflow<Message, Message> dfl = buildDataflow(kafkaZkConnect);
    //Get the dataflow's descriptor
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    //Output the descriptor in human-readable JSON
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));

    //Ensure all your sources and sinks are up and running first, then...

    //Submit the dataflow and wait until it starts running
    new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForRunning(60000);
    
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
  
}
