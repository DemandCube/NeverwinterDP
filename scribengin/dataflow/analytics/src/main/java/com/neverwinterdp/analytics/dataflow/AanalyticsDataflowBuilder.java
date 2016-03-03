package com.neverwinterdp.analytics.dataflow;

import com.neverwinterdp.analytics.AnalyticsConfig;
import com.neverwinterdp.analytics.ads.ADSEvent;
import com.neverwinterdp.analytics.ads.ADSEventStatisticOperator;
import com.neverwinterdp.analytics.odyssey.Event;
import com.neverwinterdp.analytics.odyssey.OdysseyEventStatisticOperator;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.analytics.web.WebEventJunkOperator;
import com.neverwinterdp.analytics.web.WebEventStatisticOperator;
import com.neverwinterdp.message.TrackingWindowReport;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.KafkaWireDataSetFactory;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.es.ESStorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.storage.nulldev.NullDevStorageConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.client.VMClient;

public class AanalyticsDataflowBuilder {
  AnalyticsConfig config = new AnalyticsConfig();
  
  public AanalyticsDataflowBuilder() {
  }
  
  public AanalyticsDataflowBuilder(AnalyticsConfig config) {
    this.config = config;
  }
  
  public Dataflow buildDataflow() {
    Dataflow dfl = new Dataflow(config.dataflowId);
    dfl.
      setDFSAppHome(config.dfsAppHome).
      useWireDataSetFactory(new KafkaWireDataSetFactory(config.zkConnect)).
      setDefaultParallelism(config.dataflowDefaultParallelism).
      setDefaultReplication(config.dataflowDefaultReplication).
      setTrackingWindowSize(config.dataflowTrackingWindowSize).
      setSlidingWindowSize(config.dataflowSlidingWindowSize);
    dfl.getWorkerDescriptor().setNumOfInstances(config.dataflowNumOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(config.dataflowNumOfExecutorPerWorker);
    
    KafkaDataSet<WebEvent> odysseyEventInputDs = 
      dfl.createInput(new KafkaStorageConfig("odyssey.input", config.zkConnect, config.generatorOdysseyInputTopic));
    
    KafkaDataSet<WebEvent> webEventInputDs = 
      dfl.createInput(new KafkaStorageConfig("web.input", config.zkConnect, config.generatorWebInputTopic));
    
    KafkaDataSet<WebEvent> adsEventInputDs = 
        dfl.createInput(new KafkaStorageConfig("ads.input", config.zkConnect, config.dataflowADSInputTopic));
    
    DataSet<WebEvent> nullDevDs = dfl.createOutput(new NullDevStorageConfig());
    
    ESStorageConfig esOdysseyOutputStorageConfig = 
        new ESStorageConfig("odyssey.output", config.dataflowOdysseyOutputIndex, config.esAddresses, Event.class);
    DataSet<WebEvent> esOdysseyOutputDs = dfl.createOutput(esOdysseyOutputStorageConfig);
    
    ESStorageConfig esADSOutputStorageConfig = 
        new ESStorageConfig("ads.output", "ads-unique-visitor", config.esAddresses, ADSEvent.class);
    DataSet<WebEvent> esADSOutputDs = dfl.createOutput(esADSOutputStorageConfig);
    
    Operator<WebEvent, WebEvent> routerOp   = dfl.createOperator("router", RouterOperator.class);
    Operator<WebEvent, WebEvent> webStatisticOp  = dfl.createOperator("web.statistic", WebEventStatisticOperator.class);
    Operator<WebEvent, WebEvent> adsStatisticOp  = dfl.createOperator("ads.statistic", ADSEventStatisticOperator.class);
    Operator<WebEvent, WebEvent> odysseyStatisticOp = dfl.createOperator("odyssey.statistic", OdysseyEventStatisticOperator.class);
    Operator<WebEvent, WebEvent> webJunkOp       = dfl.createOperator("web.junk", WebEventJunkOperator.class);
    
    odysseyEventInputDs.useRawReader().connect(routerOp);
    webEventInputDs.useRawReader().connect(routerOp);
    adsEventInputDs.useRawReader().connect(routerOp);
    
    routerOp.
      connect(webJunkOp).
      connect(webStatisticOp).
      connect(adsStatisticOp).
      connect(odysseyStatisticOp);
    
    webJunkOp.connect(nullDevDs);
    webStatisticOp.connect(nullDevDs);
    
    adsStatisticOp.connect(esADSOutputDs);
    
    odysseyStatisticOp.connect(esOdysseyOutputDs);
    return dfl;
  }
  
  /**
   * The logic to submit the dataflow
   * @throws Exception
   */
  public void submitDataflow(ScribenginShell shell) throws Exception {
    //Upload our app to HDFS
    VMClient vmClient = shell.getScribenginClient().getVMClient();
    vmClient.uploadApp(config.localAppHome, config.dfsAppHome);
    
    Dataflow dfl = buildDataflow();
    //Get the dataflow's descriptor
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    //Output the descriptor in human-readable JSON
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));

    //Ensure all your sources and sinks are up and running first, then...

    //Submit the dataflow and wait until it starts running
    DataflowSubmitter submitter = new DataflowSubmitter(shell.getScribenginClient(), dfl);
    submitter.submit().waitForDataflowRunning(60000);
  }
  
  public void runMonitor(ScribenginShell shell, long numOfInputMessages, boolean shutdownDataflow) throws Exception {
    ScribenginClient sclient = shell.getScribenginClient();
    DataflowClient dflClient = sclient.getDataflowClient(config.dataflowId);
    DataflowRegistry dflRegistry = dflClient.getDataflowRegistry();
    
    while(true) {
      TrackingWindowReport report = dflRegistry.getMessageTrackingRegistry().getReport();
      shell.execute("dataflow info --dataflow-id " + config.dataflowId + " --show-tasks --show-history-workers ");
      
      System.err.println(
          "numOfInputMessages = " + numOfInputMessages + 
          ", tracking count = " + report.getTrackingCount() +
          ", duplicated = " + report.getTrackingDuplicatedCount());
      if(numOfInputMessages <= report.getTrackingCount()) {
        break;
      }
      
      Thread.sleep(10000);
    }
    if(shutdownDataflow) {
      shutdownDataflow(shell);
    }
    shell.execute("dataflow info --dataflow-id " + config.dataflowId + " --show-tasks --show-history-workers");
  }
  
  public void shutdownDataflow(ScribenginShell shell) throws Exception {
    System.err.println("Should call stop the dataflow here!!!!!!!!!!!");
    shell.execute("dataflow stop --dataflow-id " + config.dataflowId);
    Thread.sleep(10000);
  }
}