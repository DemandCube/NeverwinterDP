package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class DataflowDebugger implements NodeDebugger {
  String dataflowPath ;
  
  public DataflowDebugger(String dataflowPath) {
    this.dataflowPath = dataflowPath;
  }
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    registryDebugger.println("DataflowLifecycleDebugger: Node = " + node.getPath() + ", Event = CREATE");
    DataflowFormater dflFormater = new DataflowFormater(registryDebugger.getRegistry(), dataflowPath);
    registryDebugger.println(dflFormater.getFormattedText());
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
    registryDebugger.println("DataflowLifecycleDebugger: Node = " + node.getPath() + ", Event = MODIFY");
    DataflowFormater dflFormater = new DataflowFormater(registryDebugger.getRegistry(), dataflowPath);
    registryDebugger.println(dflFormater.getFormattedText());
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}