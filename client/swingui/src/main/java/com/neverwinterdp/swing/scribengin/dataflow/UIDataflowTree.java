package com.neverwinterdp.swing.scribengin.dataflow;

import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.swing.registry.UIActivitiesView;
import com.neverwinterdp.swing.registry.UIActivityStepsView;
import com.neverwinterdp.swing.registry.UINotificationView;
import com.neverwinterdp.swing.registry.UIRegistryNodeView;
import com.neverwinterdp.swing.registry.UIRegistryTree;

@SuppressWarnings("serial")
public class UIDataflowTree extends UIRegistryTree {
  private RegistryTreeNodePathMatcher dataflowListNodeMatcher ;
  private RegistryTreeNodePathMatcher refDataflowNodeMatcher ;
  private RegistryTreeNodePathMatcher dataflowNodeMatcher ;
  
  private RegistryTreeNodePathMatcher activityNodeMatcher ;
  private RegistryTreeNodePathMatcher activityListMatcher ;
  private RegistryTreeNodePathMatcher activityQueueMatcher ;
  
  private RegistryTreeNodePathMatcher notificationNodeMatcher ;
  
  private RegistryTreeNodePathMatcher ignoreNodeMatcher ;
  
  
  public UIDataflowTree() throws Exception {
    super(ScribenginService.DATAFLOWS_PATH, "Dataflows");
    dataflowListNodeMatcher = new RegistryTreeNodePathMatcher() ;
    dataflowListNodeMatcher.add(ScribenginService.DATAFLOWS_PATH + "/(active|history|all)");
    
    refDataflowNodeMatcher = new RegistryTreeNodePathMatcher() ;
    refDataflowNodeMatcher.add(ScribenginService.DATAFLOWS_PATH + "/(active|history)/.*");
    
    
    dataflowNodeMatcher = new RegistryTreeNodePathMatcher() ;
    dataflowNodeMatcher.add(ScribenginService.DATAFLOWS_PATH + "/(active|history|all)/[^/]*$");
    
    activityNodeMatcher = new RegistryTreeNodePathMatcher() ;
    activityNodeMatcher.add(ScribenginService.DATAFLOWS_ALL_PATH + "/.*/activities/(active|history|all)/[^/]*$");
    
    activityListMatcher = new RegistryTreeNodePathMatcher() ;
    activityListMatcher.add(ScribenginService.DATAFLOWS_ALL_PATH + "/.*/activities/(active|history|all)");
    
    activityQueueMatcher = new RegistryTreeNodePathMatcher() ;
    activityQueueMatcher.add(ScribenginService.DATAFLOWS_ALL_PATH + "/.*/activities/queue");
    
    notificationNodeMatcher = new RegistryTreeNodePathMatcher() ;
    notificationNodeMatcher.add(ScribenginService.DATAFLOWS_ALL_PATH + "/.*/notifications/.*-events");
    
    ignoreNodeMatcher = new RegistryTreeNodePathMatcher() ;
    ignoreNodeMatcher.add(ScribenginService.DATAFLOWS_ALL_PATH + "/.*/activities/activity-id-tracker");
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
    System.out.println("on custom node: " + node.getNodePath());
    if(dataflowListNodeMatcher.matches(node)) {
      view.addView("Dataflow List", new UIDataflowListView(node.getNodePath()), false) ;
    } else if(dataflowNodeMatcher.matches(node)) {
      String dataflowRootPath = ScribenginService.DATAFLOWS_ALL_PATH + "/" + node.getNodeName();
      view.addView("Descriptor", new UIDataflowDescriptorView(dataflowRootPath), false) ;
      view.addView("Tasks",      new UIDataflowTaskView(dataflowRootPath), false) ;
      view.addView("Workers",    new UIDataflowWorkerView(dataflowRootPath + "/workers"), false) ;
    } else if(activityNodeMatcher.matches(node)) {
      view.addView("Activity Steps", new UIActivityStepsView(node.getNodePath(), node.getNodeName()), false) ;
    } else if(activityListMatcher.matches(node)) {
      view.addView("Activities", new UIActivitiesView(node.getNodePath()), false) ;
    } else if(activityQueueMatcher.matches(node)) {
      view.addView("Queue Activities", new UIActivitiesView(node.getNodePath()), false) ;
    } else  if(notificationNodeMatcher.matches(node)) {
      view.addView("Notifications", new UINotificationView(node.getNodePath()), false) ;
    }
    view.setSelectedView(0);
  }

  public RegistryTreeNode onCustomTreeNode(RegistryTreeNode node) {
    if(ignoreNodeMatcher.matches(node)) {
      return null ;
    } else if(refDataflowNodeMatcher.matches(node)) {
      node.setAllowsChildren(false);
    }
    return node ;
  }
}
