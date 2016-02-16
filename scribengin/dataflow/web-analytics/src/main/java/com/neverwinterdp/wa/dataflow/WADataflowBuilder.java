package com.neverwinterdp.wa.dataflow;

import com.neverwinterdp.message.TrackingWindowReport;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.KafkaWireDataSetFactory;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.storage.nulldev.NullDevStorageConfig;
import com.neverwinterdp.wa.event.WebEvent;

public class WADataflowBuilder {
  private String dataflowId         = "web-analytics";
  private int    defaultParallelism = 5;
  private int    defaultReplication = 1;

  private int trackingWindowSize     = 1000;
  private int slidingWindowSize      = 15;
  
  private int    numOfWorker            = 2;
  private int    numOfExecutorPerWorker = 4;
  
  public Dataflow<WebEvent, WebEvent> buildDataflow() {
    Dataflow<WebEvent, WebEvent> dfl = new Dataflow<>(dataflowId);
    dfl.
      useWireDataSetFactory(new KafkaWireDataSetFactory("127.0.0.1:2181")).
      setDefaultParallelism(defaultParallelism).
      setDefaultReplication(defaultReplication).
      setTrackingWindowSize(trackingWindowSize).
      setSlidingWindowSize(slidingWindowSize);
    dfl.getWorkerDescriptor().setNumOfInstances(numOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(numOfExecutorPerWorker);
    
    KafkaDataSet<WebEvent> inputDs = dfl.createInput(new KafkaStorageConfig("input", "127.0.0.1:2181", "user.click"));
    
    DataSet<WebEvent> nullDevDs = dfl.createOutput(new NullDevStorageConfig());
    
    Operator<WebEvent, WebEvent> routerOp   = dfl.createOperator("router", RouterOperator.class);
    Operator<WebEvent, WebEvent> hitStatOp  = dfl.createOperator("hit-stats", HitStatOperator.class);
    Operator<WebEvent, WebEvent> junkStatOp = dfl.createOperator("junk-stats", JunkStatOperator.class);

    inputDs.
      useRawReader().
      connect(routerOp);
    
    routerOp.
      connect(junkStatOp).
      connect(hitStatOp);
    
    junkStatOp.connect(nullDevDs);
    hitStatOp.connect(nullDevDs);

    return dfl;
  }
  
  public void runMonitor(ScribenginShell shell, long numOfInputMessages) throws Exception {
    ScribenginClient sclient = shell.getScribenginClient();
    DataflowClient dflClient = sclient.getDataflowClient(dataflowId);
    DataflowRegistry dflRegistry = dflClient.getDataflowRegistry();
    
    while(true) {
      TrackingWindowReport report = dflRegistry.getMessageTrackingRegistry().getReport();
      shell.execute("dataflow info --dataflow-id " + dataflowId + " --show-tasks --show-history-workers ");
      
      System.err.println(
          "numOfInputMessages = " + numOfInputMessages + 
          ", tracking count = " + report.getTrackingCount() +
          ", duplicated = " + report.getTrackingDuplicatedCount());
      if(numOfInputMessages <= report.getTrackingCount()) {
        break;
      }
      
      Thread.sleep(10000);
    }
    
    System.err.println("Should call stop the dataflow here!!!!!!!!!!!");
    shell.execute("dataflow stop --dataflow-id " + dataflowId);

    Thread.sleep(10000);
    shell.execute("dataflow info --dataflow-id " + dataflowId + " --show-tasks --show-history-workers");
  }
}