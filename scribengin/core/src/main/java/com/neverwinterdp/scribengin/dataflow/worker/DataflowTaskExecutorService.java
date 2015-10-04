package com.neverwinterdp.scribengin.dataflow.worker;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.registry.txevent.TXEventNotification;
import com.neverwinterdp.registry.txevent.TXEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricRegistry;

@Singleton
public class DataflowTaskExecutorService {
  private Logger logger ;

  @Inject
  private VMDescriptor vmDescriptor ;

  @Inject
  private DataflowRegistry dataflowRegistry ;

  @Inject
  private MetricRegistry   metricRegistry ;
  
  @Inject
  private SourceFactory sourceFactory;
  
  @Inject
  private SinkFactory   sinkFactory;
  
  @Inject
  private LoggerFactory loggerFactory ;
  
  private Notifier notifier ;
  
  private DataflowWorkerEventWatcher   dataflowWorkerEventWatcher ;
  
  private DataflowDescriptor dataflowDescriptor;
  private DataflowTaskExecutors taskExecutors = new DataflowTaskExecutors();
  private DataflowWorkerStatus workerStatus = DataflowWorkerStatus.INIT;
  
  private boolean kill = false ;
  
  public DataflowRegistry getDataflowRegistry() { return this.dataflowRegistry ; }
  
  public MetricRegistry getMetricRegistry() { return this.metricRegistry; }
  
  public SourceFactory getSourceFactory() { return this.sourceFactory; }
  
  public SinkFactory   getSinkFactory() { return this.sinkFactory; }
  
  public VMDescriptor getVMDescriptor() { return this.vmDescriptor; }

  public DataflowTaskExecutors getDataflowTaskExecutors() { return this.taskExecutors; }
  
  public Logger getLogger() { return logger ; }
  
  @PostConstruct
  public void onInit() throws Exception {
    logger = loggerFactory.getLogger(DataflowTaskExecutorService.class) ;
    logger.info("Start onInit()");
    Node workerNode = dataflowRegistry.getWorkerRegistry().getWorkerNode(vmDescriptor.getId()) ;
    notifier = new Notifier(dataflowRegistry.getRegistry(),  workerNode.getPath() + "/notification", "dataflow-executor-service");
    notifier.initRegistry();
    
    TXEventBroadcaster broadcaster = dataflowRegistry.getWorkerRegistry().getWorkerEventBroadcaster();
    String workerEvtPath = broadcaster.getEventPath();
    dataflowWorkerEventWatcher = new DataflowWorkerEventWatcher(dataflowRegistry, workerEvtPath, vmDescriptor.getId());
    dataflowDescriptor = dataflowRegistry.getDataflowDescriptor();
    
    int numOfExecutors = dataflowDescriptor.getNumberOfExecutorsPerWorker();
    for(int i = 0; i < numOfExecutors; i++) {
      DataflowTaskExecutorDescriptor descriptor = new DataflowTaskExecutorDescriptor ("executor-" + i);
      if(dataflowDescriptor.getDataflowTaskExecutorType() == DataflowDescriptor.DataflowTaskExecutorType.Dedicated) {
        taskExecutors.add(new DataflowTaskDedicatedExecutor(this, descriptor));
      } else {
        taskExecutors.add(new DataflowTaskSwitchableExecutor(this, descriptor));
      }
    }
    logger.info("Finish onInit()");
  }
  
  public void start() throws Exception {
    logger.info("Start start()");
    notifier.info("start-start", "DataflowTaskExecutorService: start start()");
    taskExecutors.start();
    workerStatus = DataflowWorkerStatus.RUNNING;
    dataflowRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    notifier.info("finish-start", "DataflowTaskExecutorService: finish start()");
    logger.info("Finish start()");
  }
  
  
  void interrupt() throws Exception {
    taskExecutors.interrupt();
    taskExecutors.waitForTermination();
  }
  
  public void pause() throws Exception {
    logger.info("start pause()");
    notifier.info("start-pause", "DataflowTaskExecutorService: start pause()");
    workerStatus = DataflowWorkerStatus.PAUSING;
    dataflowRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    interrupt();
    
    workerStatus = DataflowWorkerStatus.PAUSE;
    dataflowRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    notifier.info("finish-pause", "DataflowTaskExecutorService: finish pause()");
    logger.info("finish pause()");
  }
 
  @PreDestroy
  public void shutdown() throws Exception {
    if(kill) return;
    if(workerStatus == DataflowWorkerStatus.TERMINATED || workerStatus == DataflowWorkerStatus.TERMINATED_WITH_ERROR ) {
      return;
    }
    logger.info("Start shutdown()");
    notifier.info("start-shutdown", "DataflowTaskExecutorService: start shutdown()");

    workerStatus = DataflowWorkerStatus.TERMINATING;
    dataflowRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);
    dataflowWorkerEventWatcher.setComplete();
    if(taskExecutors.countAlive() > 0) {
      interrupt() ;
    }
    workerStatus = DataflowWorkerStatus.TERMINATED;
    dataflowRegistry.getWorkerRegistry().setWorkerStatus(vmDescriptor, workerStatus);

    notifier.info("finish-shutdown", "DataflowTaskExecutorService: finish shutdown()");
    logger.info("Finish shutdown()");
  }
  
  public void simulateKill() throws Exception {
    logger.info("Start kill()");
    notifier.info("start-simulate-kill", "DataflowTaskExecutorService: start simulateKill()");
    kill = true ;
    if(workerStatus != DataflowWorkerStatus.TERMINATED) {
      taskExecutors.simulateKill();
    }
    notifier.info("finish-simulate-kill", "DataflowTaskExecutorService: finish simulateKill()");
    logger.info("Finish kill()");
  }
  
  
  synchronized public void waitForTerminated(long checkPeriod) throws Exception {
    while(true) {
      taskExecutors.waitForTermination();
      if(workerStatus == DataflowWorkerStatus.RUNNING) {
        shutdown();
        return ;
      } else if(workerStatus == DataflowWorkerStatus.TERMINATED || 
                workerStatus == DataflowWorkerStatus.TERMINATED_WITH_ERROR) {
        return ;
      }
      Thread.sleep(checkPeriod);
    }
  }
  
  public class DataflowWorkerEventWatcher extends TXEventWatcher {
    public DataflowWorkerEventWatcher(DataflowRegistry dflRegistry, String eventsPath, String clientId) throws RegistryException {
      super(dflRegistry.getRegistry(), eventsPath, clientId);
    }
    
    public void onTXEvent(TXEvent txEvent) throws Exception {
      DataflowEvent taskEvent = txEvent.getDataAs(DataflowEvent.class);
      if(taskEvent == DataflowEvent.PAUSE) {
        logger.info("Dataflow worker detect pause event!");
        pause() ;
      } else if(taskEvent == DataflowEvent.STOP) {
        logger.info("Dataflow worker detect stop event!");
        shutdown() ;
      } else if(taskEvent == DataflowEvent.RESUME) {
        logger.info("Dataflow worker detect resume event!");
        start() ;
      }
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }
}