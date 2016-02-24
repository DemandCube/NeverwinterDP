package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.txevent.TXEventBroadcaster;
import com.neverwinterdp.scribengin.dataflow.runtime.master.DataflowMasterRuntimeReport;
import com.neverwinterdp.vm.VMDescriptor;

public class MasterRegistry {
  final static public String MASTER_EVENT_PATH  = "master/events" ;
  
  private Registry registry ;
  private String   dataflowPath;
  
  private Node     masterNode ;
  private Node     masterLeaderNode ;
  private Node     activitiesNode;
  
  private TXEventBroadcaster masterEventBroadcaster;
  
  public MasterRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry    = registry;
    this.dataflowPath = dataflowPath;
    masterNode       = registry.get(dataflowPath + "/master");
    masterLeaderNode = registry.get(dataflowPath + "/master/leader");
    activitiesNode   = registry.get(dataflowPath + "/master/activities");
    masterEventBroadcaster = new TXEventBroadcaster(registry, dataflowPath + "/" + MASTER_EVENT_PATH, false);
  }
  
  void create(Transaction transaction) throws RegistryException {
    transaction.create(masterNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create(masterLeaderNode.getPath(),new byte[0], NodeCreateMode.PERSISTENT);
    masterEventBroadcaster.initRegistry(transaction);
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
  }
  
  public String getActivitiesNodePath() { return activitiesNode.getPath(); }
  
  public Node getActivitiesNode() { return activitiesNode ; }
  
  public TXEventBroadcaster getMasterEventBroadcaster() { return masterEventBroadcaster; }
  
  public VMDescriptor getLeaderVMDescriptor() throws RegistryException {
    RefNode refNode = masterLeaderNode.getDataAsWithDefault(RefNode.class, null);
    if(refNode != null) {
      return registry.getDataAs(refNode.getPath(), VMDescriptor.class);
    }
    return null;
  }
  
  public List<VMDescriptor> getMasterVMDescriptors() throws RegistryException {
    List<RefNode> refChildren = masterLeaderNode.getChildrenAs(RefNode.class);
    List<VMDescriptor> holder = new ArrayList<>();
    for(int i = 0; i < refChildren.size(); i++) {
      RefNode refNode = refChildren.get(i);
      VMDescriptor vmDescriptor = registry.getDataAs(refNode.getPath(), VMDescriptor.class);
      holder.add(vmDescriptor);
    }
    return holder;
  }
  
  public VMDescriptor findActiveMaster(String vmId) throws RegistryException {
    for(VMDescriptor sel : getMasterVMDescriptors()) {
      if(vmId.equals(sel.getVmId())) return sel;
    }
    return null;
  }
  
  
  public List<DataflowMasterRuntimeReport> getDataflowMasterRuntimeReports() throws RegistryException {
    return getDataflowMasterRuntimeReports(registry, dataflowPath);
  }
  
  static public List<DataflowMasterRuntimeReport> getDataflowMasterRuntimeReports(Registry registry, String dataflowPath) throws RegistryException {
    Node masterLeaderNode = registry.get(dataflowPath + "/master/leader");
    RefNode vmLeaderRef = masterLeaderNode.getDataAs(RefNode.class);
    VMDescriptor vmLeader = registry.getDataAs(vmLeaderRef.getPath(), VMDescriptor.class);
    
    List<RefNode> refChildren = masterLeaderNode.getChildrenAs(RefNode.class);
    List<DataflowMasterRuntimeReport> holder = new ArrayList<>();
    for(int i = 0; i < refChildren.size(); i++) {
      RefNode refNode = refChildren.get(i);
      VMDescriptor vmDescriptor = registry.getDataAs(refNode.getPath(), VMDescriptor.class);
      boolean leader = false;
      if(vmLeader != null) leader = vmLeader.getVmId().equals(vmDescriptor.getVmId());
      DataflowMasterRuntimeReport report = new DataflowMasterRuntimeReport();
      report.setVmId(vmDescriptor.getVmId());
      report.setLeader(leader);
      holder.add(report);
    }
    return holder;
  }
}
