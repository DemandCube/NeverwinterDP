package com.neverwinterdp.scribengin.dataflow.activity;

import com.google.inject.Injector;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class DataflowActivityService extends ActivityService {
  
  public DataflowActivityService(Injector container, DataflowRegistry dataflowRegistry) throws Exception {
    init(container, dataflowRegistry.getActiveActivitiesNode().getPath());
  }
  
}