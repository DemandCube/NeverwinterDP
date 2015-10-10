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
  private DataflowRegistry dflRegistry;
  
  @Inject
  private SourceFactory sourceFactory ;
  
  @Inject
  private SinkFactory sinkFactory ;
  
  private DataflowActivityService activityService;
  
  private TaskService<DataflowTaskDescriptor> taskService ;
  
  private DataflowTaskMonitor dataflowTaskMonitor;
  
  private DataflowWorkerMonitor  dataflowWorkerMonitor ;
  
  private DataflowTaskMasterEventWatcher eventWatcher ;
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dflRegistry; }

  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  @Inject
  public void onInject(Injector container, LoggerFactory lfactory) throws Exception {
    logger = lfactory.getLogger(DataflowService.class);
    activityService = new DataflowActivityService(container, dflRegistry) ;
  }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    dflRegistry.addAvailableTask(taskDescriptor);
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    dataflowWorkerMonitor.addWorker(vmDescriptor);
  }
  
  public void run() throws Exception {
    boolean initRegistry = dflRegistry.initRegistry();
    dataflowWorkerMonitor = new DataflowWorkerMonitor(dflRegistry, activityService);
    
    String evtPath = 
      dflRegistry.getMasterRegistry().getMasterEventBroadcaster().getEventPath();
    eventWatcher = new DataflowTaskMasterEventWatcher(dflRegistry, evtPath, vmConfig.getName());
    
    dataflowTaskMonitor = new DataflowTaskMonitor();
    taskService = new TaskService<>(dflRegistry.getTaskRegistry());
    taskService.addTaskMonitor(dataflowTaskMonitor);
    
//    Activity activity = 
//        new AllocateDataflowMasterActivityBuilder().build(dflRegistry.getDataflowPath()) ;
//    activityService.queue(activity);
    
    if(initRegistry) {
      activityService.queue(new DataflowInitActivityBuilder().build());
      activityService.queue(new DataflowRunActivityBuilder().build());
    }
  }
  
  
  public void waitForTermination() throws Exception {
    long maxRunTime = dflRegistry.getDataflowDescriptor(false).getMaxRunTime();
    if(!dataflowTaskMonitor.waitForAllTaskFinish(maxRunTime)) {
      activityService.queue(new DataflowStopActivityBuilder().build());
      dataflowTaskMonitor.waitForAllTaskFinish(-1);
    }
    dataflowWorkerMonitor.waitForAllWorkerTerminated();
    //finish
    dflRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
  
  public void simulateKill() throws Exception {
    Registry registry = dflRegistry.getRegistry();
    registry.shutdown();
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    String vmTGName = "VM-" + vmConfig.getName();
    for(Thread selThread : threadSet) {
      ThreadGroup tGroup = selThread.getThreadGroup();
      if(tGroup != null && vmTGName.equals(tGroup.getName())) {
        try {
          selThread.interrupt();
        } catch(Exception ex) {
        }
      }
    }
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
        DataflowLifecycleStatus currentStatus = dflRegistry.getStatus();
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
