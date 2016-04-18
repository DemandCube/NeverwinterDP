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
import com.neverwinterdp.vm.client.VMClient;

public class ExampleHdfsDataflowSubmitter {
  private String dataflowID;
  private int    defaultReplication;
  private int    defaultParallelism;

  private int numOfWorker;
  private int numOfExecutorPerWorker;

  private String inputTopic;

  private String hdfsLocation;

  private ScribenginShell   shell;
  private DataflowSubmitter submitter;
  
  String localAppHome;
  String dfsAppHome;
  
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
    
    //Where in HDFS to store our data
    hdfsLocation = props.getProperty("dataflow.hdfsLocation", "build/working/storage/hdfs/output");
    
    
    //The example hdfs dataflow local location
    localAppHome = props.getProperty("dataflow.localapphome", "N/A");
    
    //DFS location to upload the example dataflow
    dfsAppHome = props.getProperty("dataflow.dfsAppHome", "/applications/dataflow/hdfsexample");
  }
  
  /**
   * The logic to submit the dataflow
   * @param kafkaZkConnect [host]:[port] of Kafka's Zookeeper conenction 
   * @throws Exception
   */
  public void submitDataflow(String kafkaZkConnect) throws Exception{
    //Upload the dataflow to HDFS
    VMClient vmClient = shell.getScribenginClient().getVMClient();
    vmClient.uploadApp(localAppHome, dfsAppHome);
    
    Dataflow dfl = buildDataflow(kafkaZkConnect);
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
  public Dataflow buildDataflow(String kafkaZkConnect){
    //Create the new Dataflow object
    // <Message,Message> pertains to the <input,output> object for the data
    Dataflow dfl = new Dataflow(dataflowID);
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
    DataSet<Message> outputDs = dfl.createOutput(new HDFSStorageConfig("output", hdfsLocation));
    
    //Define which operator to use.  
    //This will be the logic that ties the input to the output
    Operator operator     = dfl.createOperator("simpleOperator", SimpleDataStreamOperator.class);
    
    //Connect your input to the operator
    inputDs.useRawReader().connect(operator);
    //Connect your operator to the output
    operator.connect(outputDs);

    return dfl;
  }
  
  
  public String getDataflowID() { return dataflowID; }

  public String getInputTopic() { return inputTopic; }
  public String getHDFSLocation() { return hdfsLocation; }
}
