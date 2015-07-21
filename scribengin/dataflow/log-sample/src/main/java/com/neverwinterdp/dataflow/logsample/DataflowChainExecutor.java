package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.chain.DataflowChainConfig;
import com.neverwinterdp.scribengin.dataflow.chain.DataflowChainSubmitter;
import com.neverwinterdp.scribengin.dataflow.chain.OrderDataflowChainSubmitter;
import com.neverwinterdp.scribengin.shell.Executor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;

public class DataflowChainExecutor extends Executor {
  private LogSampleConfig config;
  private DataflowChainConfig dflChainconfig ;
  private DataflowChainSubmitter submitter ;
  
  public DataflowChainExecutor(ScribenginShell shell,LogSampleConfig config) throws Exception {
    super(shell);
    this.config = config;
    
    String json = IOUtil.getFileContentAsString(config.dataflowDescriptor) ;
    dflChainconfig = JSONSerializer.INSTANCE.fromString(json, DataflowChainConfig.class);
    for(int i = 0; i < dflChainconfig.getDescriptors().size(); i++) {
      DataflowDescriptor descriptor = dflChainconfig.getDescriptors().get(i);
      long stopTime = 5 * descriptor.getTaskSwitchingPeriod() ;
      if(stopTime < 60000) stopTime = 60000 ;
      long maxRuntime = config.dataflowWaitForTerminationTimeout - stopTime;
      if(maxRuntime < 30000) maxRuntime = 30000;
      descriptor.setMaxRunTime(maxRuntime);
    }
    submitter = new OrderDataflowChainSubmitter(shell.getScribenginClient(), config.dfsAppHome, dflChainconfig);
    if(config.dataflowTaskDebug) {
      submitter.enableDataflowTaskDebugger();
    }
  }
  
  public DataflowChainConfig getDataflowChainConfig() { return dflChainconfig; }
  
  public DataflowChainSubmitter getDataflowChainSubmitter() { return submitter; }
  
  @Override
  public void run() {
    long start = System.currentTimeMillis() ;
    System.out.println("Submit The Dataflow Chain");
    try {
      submitter.submit(config.dataflowWaitForSubmitTimeout);
      submitter.waitForTerminated(config.dataflowWaitForTerminationTimeout);
      System.out.println("Finish The Dataflow Chain");
      System.out.println("Execute Time: " + (System.currentTimeMillis() - start) + "ms") ;
      submitter.report(System.out);
    } catch(Throwable ex) {
      ex.printStackTrace();
    }
  }
}
