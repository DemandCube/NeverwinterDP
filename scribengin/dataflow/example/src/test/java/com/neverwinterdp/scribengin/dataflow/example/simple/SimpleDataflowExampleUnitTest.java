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