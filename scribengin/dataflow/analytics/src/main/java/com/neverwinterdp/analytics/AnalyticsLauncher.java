package com.neverwinterdp.analytics;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.analytics.dataflow.AanalyticsDataflowBuilder;
import com.neverwinterdp.analytics.gripper.GripperServer;
import com.neverwinterdp.analytics.gripper.generator.EventGeneratorServer;
import com.neverwinterdp.analytics.odyssey.generator.OdysseyEventGeneratorServer;
import com.neverwinterdp.kafka.KafkaAdminTool;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.YarnVMClient;

public class AnalyticsLauncher {
  static public void main(String[] args) throws Exception {
    AnalyticsConfig config = new AnalyticsConfig();
    new JCommander(config, args);
    
    //Create a registry configuration and point it to our running Registry (Zookeeper)
    RegistryConfig registryConfig = RegistryConfig.getDefault();
    registryConfig.setConnect(config.zkConnect);
    Registry registry = new RegistryImpl(registryConfig).connect();
    
    //Configure where our hadoop master lives
    String hadoopMaster = config.hadoopMasterConnect;
    HadoopProperties hadoopProps = new HadoopProperties() ;
    hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
    hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
    
    //Set up our connection to Scribengin
    YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    ScribenginShell shell = new ScribenginShell(vmClient) ;
    shell.attribute(HadoopProperties.class, hadoopProps);

    KafkaAdminTool adminTool = new KafkaAdminTool("admin", config.zkConnect);
    if(!adminTool.topicExits(config.dataflowWebInputTopic)) {
      adminTool.createTopic(config.dataflowWebInputTopic, 2, 5);
    }
    
    if(!adminTool.topicExits(config.dataflowADSInputTopic)) {
      adminTool.createTopic(config.dataflowADSInputTopic, 2, 5);
    }
    
    String[] gripperServerConfig = {
      "--num-of-workers", "3", "--kafka-zk-connects", config.zkConnect
    };
    GripperServer  server = new GripperServer(gripperServerConfig);
    server.start();
    
    String[] odysseyGeneratorConfig = {
      "--num-of-workers", "1", "--zk-connects", config.zkConnect, 
      "--topic", config.generatorOdysseyInputTopic, "--replication", "2", "--partition", "5", 
      "--num-of-events", Integer.toString(config.generatorOdysseyNumOfEvents)
    };
    OdysseyEventGeneratorServer odysseyEventGeneratorServer = new OdysseyEventGeneratorServer(odysseyGeneratorConfig); 
    odysseyEventGeneratorServer.start();

    String[] webEventGeneratorConfig = {
      "--gripper-server-host", "127.0.0.1", "--gripper-server-port", "7081",
      "--num-of-threads", Integer.toString(config.generatorWebNumOfThreads),
      "--thread-sleep",   Long.toString(config.generatorWebThreadSleep),
      "--num-of-pages",   Integer.toString(config.generatorWebNumOfEvents), 
      "--max-visit-time", Integer.toString(config.generatorWebMaxVisitTime),
      "--min-visit-time", Integer.toString(config.generatorWebMinVisitTime),
      "--destination-topic", config.dataflowWebInputTopic
    };
    EventGeneratorServer wGeneratorServer = new EventGeneratorServer(webEventGeneratorConfig);
    wGeneratorServer.start();
      
    AanalyticsDataflowBuilder dflBuilder = new AanalyticsDataflowBuilder(config);
    dflBuilder.submitDataflow(shell);
    dflBuilder.runMonitor(shell, config.generatorOdysseyNumOfEvents + config.generatorWebNumOfEvents, !config.waitForInput);
  }
}