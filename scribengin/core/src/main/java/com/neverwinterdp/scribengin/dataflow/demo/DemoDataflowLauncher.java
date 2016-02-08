package com.neverwinterdp.scribengin.dataflow.demo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
import com.neverwinterdp.storage.es.ESStorageConfig;
import com.neverwinterdp.storage.es.Mapper;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;


public class DemoDataflowLauncher {
  static public class Config {
    @Parameter(names = {"--help", "-h"}, help = true, description = "Output this help message")
    private boolean help;

    @Parameter(names = "--local-app-home", required=true, description="The example dataflow local location")
    String localAppHome ;
    
    @Parameter(names = "--dfs-app-home", description="DFS location to upload the example dataflow")
    String dfsAppHome = "/applications/dataflow/demo";
    
    @Parameter(names = "--zk-connect", description="[host]:[port] of Zookeeper server")
    String zkConnect = "zookeeper-1:2181";
    
    @Parameter(names = "--hadoop-master-connect", description="Hostname of HadoopMaster")
    String hadoopMasterConnect = "hadoop-master";

    @Parameter(names = "--resourceManagerPort", description = "YARN resource manager port")
    int resourceManagerPort = 8032 ;
    
    @Parameter(names = "--hdfsPort", description = "HDFS port")
    int hdfsPort = 9000 ;
    
    @Parameter(names = "--dataflow-id", description = "Unique ID for the dataflow")
    String dataflowId        = "DemoDataflow";
    
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
    
    @Parameter(names = "--elasticsearch-host", description = "Hostname of elasticsearch")
    String elasticsearchHost = "elasticsearch-1:9300";
    
    
    @Parameter(names = "--elasticsearch-index", description = "Name of output Elasticsearch index")
    String outputIndex ="output.index";
    
    Class<Mapper> defaultMapper = com.neverwinterdp.storage.es.Mapper.class;
    @Parameter(names = "--elasticsearch-mapper", description = "Name of mapper for elasticsearch")
    String esMapper = defaultMapper.getName();
    
    
    
  }
  
  private Config config = new Config();
  private ScribenginShell shell;
  
  public DemoDataflowLauncher(ScribenginShell shell, Config conf){
    this.config = conf;
    this.shell = shell;
  }
  
  public void submitDataflow() throws Exception{
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
  
  public Dataflow<Message,Message> buildDataflow(){
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
    
    
    //Define our output sink
    DataSet<Message> outputDs =
        dfl.createOutput(new ESStorageConfig("output", config.elasticsearchHost, config.outputIndex, config.esMapper));
    
    //Define which operator to use.  
    //This will be the logic that ties the input to the output
    Operator<Message, Message> operator     = dfl.createOperator("demoOperator", DemoOperator.class);
    
    //Connect your input to the operator
    inputDs.useRawReader().connect(operator);
    //Connect your operator to the output
    operator.connect(outputDs);
    
    return dfl;
  }
  
  public static void main(String args[]) throws Exception{
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
    hadoopProps.put("yarn.resourcemanager.address", 
        hadoopMaster + ":"+Integer.toString(config.resourceManagerPort));
    hadoopProps.put("fs.defaultFS", 
        "hdfs://" + hadoopMaster +":"+Integer.toString(config.hdfsPort));
    
    //Set up our connection to Scribengin
    YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    ScribenginShell shell = new ScribenginShell(vmClient) ;
    shell.attribute(HadoopProperties.class, hadoopProps);
    
    DemoDataflowLauncher launcher = new DemoDataflowLauncher(shell, config);
    //Launch our configured dataflow
    launcher.submitDataflow();
    
    
    //Get some info on the running dataflow
    shell.execute("dataflow info --dataflow-id " + config.dataflowId);
    
    //Close connection with Scribengin
    shell.close();
    shell.console().println("Simple Example Datafow is done!!!");
    System.exit(0);
  }
}
