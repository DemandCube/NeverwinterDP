package com.neverwinterdp.scribengin.dataflow.simulation;

import java.util.List;

import javax.annotation.PostConstruct;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.registry.txevent.TXEventNotification;
import com.neverwinterdp.registry.txevent.TXEventWatcher;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig.FailurePoint;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;

@Singleton
public class FailureSimulationService {
  @Inject
  private DataflowService                   dataflowService;
  
  @Inject
  private VMConfig                          vmConfig;

  private FailureSimulationEventNodeWatcher failureEventNodeWatcher;
  private FailureConfig                     currentFailureConfig;
  
  @PostConstruct
  public void onInit() throws Exception {
    DataflowRegistry dflRegistry = dataflowService.getDataflowRegistry();
    String eventPath = 
      dflRegistry.getMasterRegistry().getFaillureSimulationEventBroadcaster().getEventPath();
    failureEventNodeWatcher = 
      new FailureSimulationEventNodeWatcher(dflRegistry, eventPath, vmConfig.getName());
    
    System.err.println("FailureSimulationService: ");
    List<TXEvent> events = 
        dflRegistry.getRegistry().getChildrenAs(eventPath, TXEvent.class);
    System.err.println("FailureSimulationService: old events = " + events.size());
    if(events.size() > 0) {
      TXEvent txEvent = events.get(events.size() - 1);
      currentFailureConfig = txEvent.getDataAs(FailureConfig.class);
      failureEventNodeWatcher.notify(txEvent, TXEventNotification.Status.Complete);
    }
  }
  
  synchronized public void runFailureSimulation(Activity activity, ActivityStep step, FailurePoint failurePoint) throws Exception {
    if(currentFailureConfig == null) return;
    if(currentFailureConfig.matches(activity) &&
       currentFailureConfig.matches(step) && 
       currentFailureConfig.matches(failurePoint)) {
      System.err.println("runFailureSimulation(...)");
      System.err.println("  json: " + JSONSerializer.INSTANCE.toString(currentFailureConfig));
      dataflowService.simulateKill();
      currentFailureConfig = null ;
    }
  }
  
  public class FailureSimulationEventNodeWatcher extends TXEventWatcher {
    public FailureSimulationEventNodeWatcher(DataflowRegistry dflRegistry, String eventsPath, String clientId) throws RegistryException {
      super(dflRegistry.getRegistry(), eventsPath, clientId);
    }
    
    public void onTXEvent(TXEvent txEvent) throws Exception {
      currentFailureConfig = txEvent.getDataAs(FailureConfig.class);
      System.out.println("FailureSimulationEventNodeWatcher: " + JSONSerializer.INSTANCE.toString(currentFailureConfig)); 
      notify(txEvent, TXEventNotification.Status.Complete);
    }
  }
}
