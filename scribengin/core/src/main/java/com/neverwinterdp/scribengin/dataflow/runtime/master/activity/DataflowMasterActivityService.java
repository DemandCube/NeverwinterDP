package com.neverwinterdp.scribengin.dataflow.runtime.master.activity;

import com.google.inject.Injector;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class DataflowMasterActivityService extends ActivityService {
  
  public DataflowMasterActivityService(Injector container, DataflowRegistry dflRegistry) throws Exception {
    init(container, dflRegistry.getMasterRegistry().getActivitiesNodePath());
  }
  
}