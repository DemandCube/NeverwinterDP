package com.neverwinterdp.dataflow.logsample;


import java.util.List;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.shell.Executor;
import com.neverwinterdp.scribengin.shell.ExecutorScheduler;
import com.neverwinterdp.scribengin.shell.GroupExecutor;
import com.neverwinterdp.scribengin.shell.RandomKillDataflowWorkerExecutor;
import com.neverwinterdp.scribengin.shell.StartStopDataflowExecutor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;
import com.neverwinterdp.yara.snapshot.ClusterMetricRegistrySnapshot;

public class LogSampleClient  {
  LogSampleConfig config;
  
  public LogSampleClient(String[] args) {
    config = new LogSampleConfig();
    new JCommander(config, args);
  }
  
  public void run() throws Exception {
    Registry registry = config.registryConfig.newInstance();
    registry.connect(30000);
    
    if(config.logValidatorValidateS3 != null) {
      S3Client s3Client = new S3Client();
      if(s3Client.hasBucket("test-log-sample")) {
        s3Client.deleteBucket("test-log-sample", true);
      }
      s3Client.createBucket("test-log-sample");
      s3Client.onDestroy();
    }
    
    VMClient vmClient = null;
    if(config.dfsAppHome != null) {
      String hadoopMaster = System.getProperty("shell.hadoop-master");
      HadoopProperties hadoopProps = new HadoopProperties() ;
      hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
      hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
      vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    } else {
      vmClient = new VMClient(registry);
    }
    ScribenginShell shell = new ScribenginShell(vmClient);
    if(config.uploadApp != null) {
      shell.getVMClient().uploadApp(config.uploadApp, config.dfsAppHome);
    }
    
    ExecutorScheduler scheduler = new ExecutorScheduler();
    GroupExecutor logGeneratorGroup = scheduler.newGroupExcecutor("log-generator");
    logGeneratorGroup.withMaxRuntime(30000).withWaitForReady(config.logGeneratorWaitForReady);;
    for(int i = 0; i < 1; i++) {
      Executor executor = new VMLogGeneratorExecutor(shell, (i + 1), config);
      logGeneratorGroup.add(executor);
    }
    
    GroupExecutor dataflowChainGroup = scheduler.newGroupExcecutor("dataflow-executor-group");
    dataflowChainGroup.withMaxRuntime(config.dataflowWaitForTerminationTimeout);
    DataflowChainExecutor dataflowChainExecutor = new DataflowChainExecutor(shell, config);
    dataflowChainGroup.add(dataflowChainExecutor);
    
    if(config.dataflowFailureSimulationWorker) {
      for(DataflowDescriptor dflDescriptor : dataflowChainExecutor.getDataflowChainConfig().getDescriptors()) {
        String dataflowId = dflDescriptor.getId();
        RandomKillDataflowWorkerExecutor executor = new RandomKillDataflowWorkerExecutor(shell, dataflowId); 
        executor.waitBeforeSimulateFailure = config.dataflowFailureSimulationWaitBeforeStart;
        executor.simulateKill = config.dataflowFailureSimulationSimulateKill;
        executor.maxKill = config.dataflowFailureSimulationMaxKill;
        executor.failurePeriod = config.dataflowFailureSimulationPeriod;
        dataflowChainGroup.add(executor);
      }
    }
    
    if(config.dataflowFailureSimulationStartStopResume) {
      for(DataflowDescriptor dflDescriptor : dataflowChainExecutor.getDataflowChainConfig().getDescriptors()) {
        String dataflowId = dflDescriptor.getId();
        if(dataflowId.indexOf("splitter") >= 0) continue;
        StartStopDataflowExecutor executor = new StartStopDataflowExecutor(shell, dataflowId); 
        dataflowChainGroup.add(executor);
      }
    }
    
    GroupExecutor validatorGroup = scheduler.newGroupExcecutor("validator-group");
    validatorGroup.withMaxRuntime(config.logValidatorWaitForTermination);
    Executor validatorExecutor = new VMLogValidatorExecutor(shell, config);
    validatorGroup.add(validatorExecutor);
    scheduler.run();
    
    List<ClusterMetricRegistrySnapshot> metrics = dataflowChainExecutor.getDataflowChainSubmitter().getMetrics();
    for(ClusterMetricRegistrySnapshot sel : metrics) {
      System.out.println("Dataflow " + sel.getClusterName() + " Report");
      System.out.println("**************************************************************************************");
      System.out.println(sel.getFormattedReport());
    }
  }
  
  static public void main(String[] args) throws Exception {
    System.setProperty("com.amazonaws.sdk.disableCertChecking", "true"); 
    LogSampleClient client = new LogSampleClient(args);
    client.run();
  }
}