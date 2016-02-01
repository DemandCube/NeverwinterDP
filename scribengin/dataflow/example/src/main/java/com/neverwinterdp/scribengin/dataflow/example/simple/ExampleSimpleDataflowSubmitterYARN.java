package com.neverwinterdp.scribengin.dataflow.example.simple;

import java.util.Properties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.YarnVMClient;

/**
 * Example class of how to launch your dataflow on a real instance of Scribengin running in YARN
 *
 */
public class ExampleSimpleDataflowSubmitterYARN {
  
  /**
   * Simple class to house our configuration options for JCommander 
   * 
   */
  static class Settings {

    @Parameter(names = {"--help", "-h"}, help = true, description = "Output this help message")
    private boolean help;
    
    @Parameter(names = "--dataflow-id", description = "Unique ID for the dataflow")
    private String dataflowId        = "ExampleDataflow";
    
    @Parameter(names = "--zkConnect", description="[host]:[port] of Zookeeper server")
    private String zkConnect = "zookeeper-1:2181";
    
    @Parameter(names = "--hadoopMasterConnect", description="Hostname of HadoopMaster")
    private String hadoopMasterConnect = "hadoop-master";
    
    @Parameter(names = "--kafkaConnect", description="[host]:[port] of kafka broker")
    private String kafkaConnect = "kafka-1:9092";
    
    @Parameter(names = "--kafkaReplication", description = "Kafka replication")
    private String kafkaReplication = "1";
    
    @Parameter(names = "--streamParallelism", description = "Number of Streams for Scribengin to deploy")
    private String streamParallelism = "8";
    
    
    @Parameter(names = "--numOfWorker", description = "Number of workers to request")
    private String numOfWorker = "2";
    
    @Parameter(names = "--numOfExecutorPerWorker", description = "Number of Executors per worker to request")
    private String numOfExecutorPerWorker     = "2";
    
    @Parameter(names = "--inputTopic", description = "Name of input Kafka Topic")
    private String inputTopic = "input.topic";
    
    @Parameter(names = "--outputTopic", description = "Name of output Kafka topic")
    private String outputTopic ="output.topic";

  }
  
  
  public static void main(String args[]) throws Exception {
    //Use JCommander to parse command line args
    Settings settings = new Settings();
    JCommander j = new JCommander(settings, args);
    
    if (settings.help) {
      j.usage();
      return;
    }
    
    //Create a registry configuration and point it to our running Registry (Zookeeper)
    RegistryConfig registryConfig = RegistryConfig.getDefault();
    registryConfig.setConnect(settings.zkConnect);
    Registry registry = null;
    try{
      registry = new RegistryImpl(registryConfig).connect();
    } catch(Exception e){
      System.err.println("Could not connect to the registry at: "+ registryConfig.getConnect()+"\n"+e.getMessage());
      return;
    }
    
    //Configure where our hadoop master lives
    String hadoopMaster = settings.hadoopMasterConnect;
    HadoopProperties hadoopProps = new HadoopProperties() ;
    hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
    hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
    
    //Set up our connection to Scribengin
    YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    ScribenginShell shell = new ScribenginShell(vmClient) ;
    shell.attribute(HadoopProperties.class, hadoopProps);
    
    //Configure our dataflow
    Properties props = new Properties();
    props.put("dataflow.id", settings.dataflowId);
    props.put("dataflow.replication", settings.kafkaReplication); 
    props.put("dataflow.parallelism", settings.streamParallelism);
    props.put("dataflow.numWorker", settings.numOfWorker);
    props.put("dataflow.numExecutorPerWorker", settings.numOfExecutorPerWorker);
    props.put("dataflow.inputTopic", settings.inputTopic);
    props.put("dataflow.outputTopic", settings.outputTopic);
    
    //Launch our configured dataflow
    ExampleSimpleDataflowSubmitter esds = new ExampleSimpleDataflowSubmitter(shell, new Properties());
    esds.submitDataflow(settings.kafkaConnect);
    
    //Get some info on the running dataflow
    shell.execute("dataflow info --dataflow-id "+settings.dataflowId+" --show-all");
    
    //Close connection with Scribengin
    shell.close();
  }

}
