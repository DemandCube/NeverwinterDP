package com.neverwinterdp.scribengin.dataflow.service;

import java.util.Set;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.task.TaskService;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventNotification;
import com.neverwinterdp.registry.txevent.TXEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowPauseActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowResumeActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowRunActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowStopActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
@JmxBean("role=dataflow-master, type=DataflowService, dataflowName=DataflowService")
public class DataflowService {
  private Logger logger ;
  
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dataflowRegistry;
  
  @Inject
  private SourceFactory sourceFactory ;
  
  @Inject
  private SinkFactory sinkFactory ;
  
  private DataflowActivityService activityService;
  
  private TaskService<DataflowTaskDescriptor> taskService ;
  
  private DataflowTaskMonitor dataflowTaskMonitor;
  
  private DataflowWorkerMonitor  dataflowWorkerMonitor ;
  
  private DataflowTaskMasterEventWatcher eventWatcher ;
  
  private Thread waitForTerminationThread ;
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(DataflowService.class);
    activityService = new DataflowActivityService(container, dataflowRegistry) ;
  }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    dataflowRegistry.addAvailableTask(taskDescriptor);
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    dataflowWorkerMonitor.addWorker(vmDescriptor);
  }
  
  public void run() throws Exception {
    boolean initRegistry = dataflowRegistry.initRegistry();
    dataflowWorkerMonitor = new DataflowWorkerMonitor(dataflowRegistry, activityService);
    
    //masterEventListener = new DataflowTaskMasterEventListenter(dataflowRegistry);
    String evtPath = 
      dataflowRegistry.getMasterRegistry().getMasterEventBroadcaster().getEventPath();
    eventWatcher = new DataflowTaskMasterEventWatcher(dataflowRegistry, evtPath, vmConfig.getName());
    
    dataflowTaskMonitor = new DataflowTaskMonitor();
    taskService = new TaskService<>(dataflowRegistry.getTaskRegistry());
    taskService.addTaskMonitor(dataflowTaskMonitor);
    
    if(initRegistry) {
      activityService.queue(new DataflowInitActivityBuilder().build());
      activityService.queue(new DataflowRunActivityBuilder().build());
    }
  }
  
  
  public void waitForTermination(Thread waitForTerminationThread) throws Exception {
    this.waitForTerminationThread = waitForTerminationThread;
    System.err.println("Before dataflowTaskMonitor.waitForAllTaskFinish();");
    long maxRunTime = dataflowRegistry.getDataflowDescriptor(false).getMaxRunTime();
    if(!dataflowTaskMonitor.waitForAllTaskFinish(maxRunTime)) {
      activityService.queue(new DataflowStopActivityBuilder().build());
      dataflowTaskMonitor.waitForAllTaskFinish(-1);
    }
    System.err.println("After dataflowTaskMonitor.waitForAllTaskFinish();");
    
    System.err.println("Before dataflowWorkerMonitor.waitForAllWorkerTerminated();");
    dataflowWorkerMonitor.waitForAllWorkerTerminated();
    System.err.println("After dataflowWorkerMonitor.waitForAllWorkerTerminated();");
    //finish
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
  
  public void simulateKill() throws Exception {
    Registry registry = dataflowRegistry.getRegistry();
    System.err.println("DataflowService simulate kill with registry " + registry.hashCode());
    registry.disconnect();
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    String threadGroupName = "VM-" + vmConfig.getName();
    for(Thread selThread : threadSet) {
      String groupName = selThread.getThreadGroup().getName();
      if(threadGroupName.equals(groupName)) {
        try {
          selThread.interrupt();
        } catch(Exception ex) {
        }
      }
    }
//    activityService.kill();
//    if(waitForTerminationThread != null) {
//      waitForTerminationThread.interrupt();
//    }
  }
  
  public class DataflowTaskMasterEventWatcher extends TXEventWatcher {
    public DataflowTaskMasterEventWatcher(DataflowRegistry dflRegistry, String eventsPath, String clientId) throws RegistryException {
      super(dflRegistry.getRegistry(), eventsPath, clientId);
    }
    
    public void onTXEvent(TXEvent txEvent) throws Exception {
      DataflowEvent taskEvent = txEvent.getDataAs(DataflowEvent.class);
      if(taskEvent == DataflowEvent.PAUSE) {
        Activity activity = new DataflowPauseActivityBuilder().build();
        activityService.queue(activity);
        logger.info("Queue a pause activity");
      } else if(taskEvent == DataflowEvent.STOP) {
        activityService.queue(new DataflowStopActivityBuilder().build());
        logger.info("Queue a stop activity");
      } else if(taskEvent == DataflowEvent.RESUME) {
        DataflowLifecycleStatus currentStatus = dataflowRegistry.getStatus();
        if(currentStatus == DataflowLifecycleStatus.PAUSE) {
          Activity activity = new DataflowResumeActivityBuilder().build();
          activityService.queue(activity);
          logger.info("Queue a resume pause activity");
          System.err.println("DataflowService: Resume from PAUSE") ;
        } else if(currentStatus == DataflowLifecycleStatus.STOP) {
          activityService.queue(new DataflowRunActivityBuilder().build());
          logger.info("Queue a run activity");
          System.err.println("DataflowService: Resume from STOP") ;
        }
      }
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }
}
