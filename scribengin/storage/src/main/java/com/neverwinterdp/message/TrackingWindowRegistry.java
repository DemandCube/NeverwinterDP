package com.neverwinterdp.message;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;

public class TrackingWindowRegistry {
  
  final static public String FINISHED  = "finished";
  final static public String PROGRESS  = "progress";
  
  private Registry           registry;
  private String             registryPath;

  private Node               rootNode;
  
  private Node               trackingNode;
  private Node               trackingProgressNode;
  private Node               trackingProgressMergeNode;
  private Node               trackingProgressSaveNode;
  private Node               trackingFinishedNode;

  private Node               windowNode;
  private Node               windowCommitsNode;
  private SequenceIdTracker  windowIdTracker;
  
  public TrackingWindowRegistry(Registry registry, String registryPath) throws RegistryException {
    this.registry         = registry;
    this.registryPath     = registryPath;
    
    rootNode     = registry.get(registryPath);
    trackingNode = rootNode.getChild("tracking");
    trackingProgressNode      = trackingNode.getChild(PROGRESS);
    trackingProgressSaveNode  = trackingProgressNode.getChild("save");
    trackingProgressMergeNode = trackingProgressNode.getChild("merge");
    trackingFinishedNode      = trackingNode.getChild(FINISHED);
    
    windowNode        = rootNode.getChild("window");
    windowCommitsNode = windowNode.getChild("commits");
    windowIdTracker= new SequenceIdTracker(registry, windowNode.getPath() + "/id-tracker", false);
  }
  
  public void initRegistry() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    initRegistry(transaction);
    transaction.commit();
  }
  
  public void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(rootNode, null, NodeCreateMode.PERSISTENT);

    transaction.create(trackingNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(trackingProgressNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(trackingProgressSaveNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(trackingProgressMergeNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(trackingFinishedNode, new  TrackingWindowReport("tracking"), NodeCreateMode.PERSISTENT);
    
    transaction.create(windowNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(windowCommitsNode, null, NodeCreateMode.PERSISTENT);
    windowIdTracker.initRegistry(transaction);
  }
  
  public int nextWindowId() throws RegistryException { return windowIdTracker.nextInt(); }
  
  public List<String> getProgressCommitWindows() throws RegistryException {
    return windowCommitsNode.getChildren();
  }
  
  public void saveWindow(final TrackingWindow ... windows) throws RegistryException {
    BatchOperations<Boolean> saveOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        for(TrackingWindow window : windows) {
          Node commitWindowNode = windowCommitsNode.getChild(window.toWindowIdName());
          transaction.create(commitWindowNode, window, NodeCreateMode.PERSISTENT);
        }
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveOp, 3, 5000);
  }
  
  public void saveProgress(final TrackingWindowStat ... windowStats) throws RegistryException {
    BatchOperations<Boolean> saveProgressOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        for(TrackingWindowStat windowStat:  windowStats) {
          transaction.createChild(trackingProgressSaveNode, "", windowStat, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
        }
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveProgressOp, 3, 5000);
  }

  public TrackingWindowStat[] mergeSaveProgress() throws RegistryException {
    BatchOperations<TrackingWindowStat[]> op = new BatchOperations<TrackingWindowStat[]>() {
      @Override
      public TrackingWindowStat[] execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        Map<Integer, TrackingWindowStat> windowStatMap = new HashMap<>();
        List<String> saveWindowStatIds = trackingProgressSaveNode.getChildren();
        for(int i = 0; i < saveWindowStatIds.size(); i++) {
          String saveWindowStatId = saveWindowStatIds.get(i);
          Node saveWindowStatNode = trackingProgressSaveNode.getChild(saveWindowStatId);
          TrackingWindowStat windowStat = saveWindowStatNode.getDataAs(TrackingWindowStat.class);
          TrackingWindowStat exists = windowStatMap.get(windowStat.getWindowId());
          if(exists != null) {
            exists.merge(windowStat);
          } else {
            windowStat.update();
            windowStatMap.put(windowStat.getWindowId(), windowStat);
          }
          transaction.deleteChild(trackingProgressSaveNode, saveWindowStatId);
        }
        
        for(TrackingWindowStat windowStat : windowStatMap.values()) {
          Node mergeWindowStatNode = trackingProgressMergeNode.getChild(windowStat.toWindowIdName());
          TrackingWindowStat mergeWindowStat = mergeWindowStatNode.getDataAsWithDefault(TrackingWindowStat.class, null);
          if(mergeWindowStat == null) {
            transaction.create(mergeWindowStatNode, windowStat, NodeCreateMode.PERSISTENT);
          } else {
            mergeWindowStat.merge(windowStat);
            transaction.setData(mergeWindowStatNode, mergeWindowStat);
          }
        }
        transaction.commit();
        TrackingWindowStat[] windowStats = new TrackingWindowStat[windowStatMap.size()];
        windowStatMap.values().toArray(windowStats);
        return windowStats;
      }
    };
    return registry.executeBatch(op, 3, 5000);
  }
  
  public TrackingWindowStat[] mergeProgress() throws RegistryException {
    BatchOperations<TrackingWindowStat[]> op = new BatchOperations<TrackingWindowStat[]>() {
      @Override
      public TrackingWindowStat[] execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        List<String> windowStatIds = trackingProgressMergeNode.getChildren();
        TrackingWindowStat[] windowStats = new TrackingWindowStat[windowStatIds.size()];
        for(int i = 0; i < windowStatIds.size(); i++) {
          String windowStatId = windowStatIds.get(i);
          Node windowStatNode = trackingProgressMergeNode.getChild(windowStatId);
          TrackingWindow window = windowCommitsNode.getChild(windowStatId).getDataAsWithDefault(TrackingWindow.class, null);
          windowStats[i] = windowStatNode.getDataAs(TrackingWindowStat.class);
          if(window != null) {
            windowStats[i].setWindowSize(window.getWindowSize());
            if(windowStats[i].getTrackingNoLostTo() + 1 == window.getWindowSize()) {
              windowStats[i].setComplete(true);
            }
          }
          if(windowStats[i].isComplete()) {
            transaction.delete(windowStatNode.getPath());
            transaction.createChild(trackingFinishedNode, windowStatId, windowStats[i], NodeCreateMode.PERSISTENT);
          }
        }
        transaction.commit();
        return windowStats;
      }
    };
    return registry.executeBatch(op, 3, 5000);
  }
  
  public TrackingWindowReport mergeFinished() throws RegistryException {
    final TrackingWindowReport report = trackingFinishedNode.getDataAs(TrackingWindowReport.class);
    final List<String> finishedWindows = trackingFinishedNode.getChildren();
    Collections.sort(finishedWindows);
    
    for(int i= 0; i < finishedWindows.size(); i++) {
      Node windowNode = trackingFinishedNode.getChild(finishedWindows.get(i));
      TrackingWindowStat windowStat = windowNode.getDataAs(TrackingWindowStat.class);
      report.mergeFinished(windowStat);
    }
    report.optimize();
    
    BatchOperations<Boolean> saveOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        transaction.setData(trackingFinishedNode, report);
        
        for(int i= 0; i < finishedWindows.size(); i++) {
          String windowIdName = finishedWindows.get(i);
          transaction.deleteChild(trackingFinishedNode, windowIdName);
          transaction.deleteChild(windowCommitsNode, windowIdName);
        }
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveOp, 3, 3000);
    return report;
  }
  
  public TrackingWindowReport getReport() throws RegistryException {
    TrackingWindowReport report = trackingFinishedNode.getDataAs(TrackingWindowReport.class);
    for(String sel : trackingFinishedNode.getChildren()) {
      TrackingWindowStat windowStat = 
          trackingFinishedNode.getChild(sel).getDataAsWithDefault(TrackingWindowStat.class, null);
      if(windowStat != null) report.mergeFinished(windowStat);
    }
    for(String sel : trackingProgressMergeNode.getChildren()) {
      TrackingWindowStat windowStat = 
          trackingProgressMergeNode.getChild(sel).getDataAsWithDefault(TrackingWindowStat.class, null);
      if(windowStat != null) {
        windowStat.update();
        report.mergeProgress(windowStat);
      }
    }
    return report;
  }
  
  public TrackingWindowReport merge() throws RegistryException {
    mergeSaveProgress();
    TrackingWindowStat[] progressWindowStats = mergeProgress();
    TrackingWindowReport report = mergeFinished();
    for(TrackingWindowStat sel : progressWindowStats) {
      if(sel.isComplete()) continue;
      report.mergeProgress(sel);
    }
    return report;
  }
}
