package com.neverwinterdp.dataflow.logsample;


import com.beust.jcommander.JCommander;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.Executor;
import com.neverwinterdp.scribengin.shell.ExecutorScheduler;
import com.neverwinterdp.scribengin.shell.GroupExecutor;
import com.neverwinterdp.scribengin.shell.RandomDataflowWorkerKiller;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;

public class LogSampleClient  {
  LogSampleConfig config;
  
  public LogSampleClient(String[] args) {
    config = new LogSampleConfig();
    new JCommander(config, args);
  }
  
  public void run() throws Exception {
    Registry registry = config.registryConfig.newInstance();
    registry.connect();
    
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
    logGeneratorGroup.withMaxRuntime(5000).withWaitForReady(5000);;
    for(int i = 0; i < 1; i++) {
      Executor executor = new VMLogGeneratorExecutor(shell, (i + 1), config);
      logGeneratorGroup.add(executor);
    }
    
    GroupExecutor dataflowChainGroup = scheduler.newGroupExcecutor("dataflow-executor-group");
    dataflowChainGroup.withMaxRuntime(config.dataflowWaitForTerminationTimeout);
    DataflowChainExecutor dataflowChainExecutor = new DataflowChainExecutor(shell, config);
    dataflowChainGroup.add(dataflowChainExecutor);
    
    for(DataflowDescriptor dflDescriptor : dataflowChainExecutor.getDataflowChainConfig().getDescriptors()) {
      String dataflowId = dflDescriptor.getId();
      RandomDataflowWorkerKiller dataflowWorkerKiller = new RandomDataflowWorkerKiller(shell, dataflowId); 
      dataflowChainGroup.add(dataflowWorkerKiller);
    }
    
    GroupExecutor validatorGroup = scheduler.newGroupExcecutor("validator-group");
    validatorGroup.withMaxRuntime(config.logValidatorWaitForTermination);
    Executor validatorExecutor = new VMLogValidatorExecutor(shell, config);
    validatorGroup.add(validatorExecutor);
    scheduler.run();
  }
  
  static public void main(String[] args) throws Exception {
    LogSampleClient client = new LogSampleClient(args);
    client.run();
  }
}