package com.neverwinterdp.analytics.dataflow;

import com.neverwinterdp.analytics.odyssey.OdysseyEventStatisticOperator;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.analytics.web.WebEventJunkOperator;
import com.neverwinterdp.analytics.web.WebEventStatisticOperator;
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

public class AanalyticsDataflowBuilder {
  private String dataflowId         = "web-analytics";
  
  private int    defaultParallelism = 5;
  private int    defaultReplication = 1;

  private int    trackingWindowSize     = 1000;
  private int    slidingWindowSize      = 15;
  
  private int    numOfWorker            = 2;
  private int    numOfExecutorPerWorker = 7;
  
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
    
    KafkaDataSet<WebEvent> odysseyEventInputDs = 
      dfl.createInput(new KafkaStorageConfig("odyssey.input", "127.0.0.1:2181", "odyssey.input"));
    KafkaDataSet<WebEvent> webEventInputDs = 
      dfl.createInput(new KafkaStorageConfig("web.input", "127.0.0.1:2181", "user.click"));
    
    DataSet<WebEvent> nullDevDs = dfl.createOutput(new NullDevStorageConfig());
    
    Operator<WebEvent, WebEvent> routerOp   = dfl.createOperator("router", RouterOperator.class);
    Operator<WebEvent, WebEvent> wStatisticOp  = dfl.createOperator("web.statistic", WebEventStatisticOperator.class);
    Operator<WebEvent, WebEvent> wJunkOp       = dfl.createOperator("web.junk", WebEventJunkOperator.class);
    Operator<WebEvent, WebEvent> odysseyStatisticOp = dfl.createOperator("odyssey.statistic", OdysseyEventStatisticOperator.class);
    
    odysseyEventInputDs.useRawReader().connect(routerOp);
    
    webEventInputDs.useRawReader().connect(routerOp);
    
    routerOp.
      connect(wJunkOp).
      connect(wStatisticOp).
      connect(odysseyStatisticOp);
    
    wJunkOp.connect(nullDevDs);
    wStatisticOp.connect(nullDevDs);
    
    odysseyStatisticOp.connect(nullDevDs);
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