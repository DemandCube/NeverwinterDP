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
import com.neverwinterdp.vm.VMDescriptor;

public class MasterRegistry {
  final static public String MASTER_EVENT_PATH  = "master/events" ;
  
  private Registry registry ;
  
  private Node     masterNode ;
  private Node     masterLeaderNode ;
  private Node     activitiesNode;
  
  private TXEventBroadcaster masterEventBroadcaster;
  
  public MasterRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry         = registry;
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
}
