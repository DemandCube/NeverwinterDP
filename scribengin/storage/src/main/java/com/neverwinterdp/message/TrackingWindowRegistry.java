package com.neverwinterdp.message;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingWindowRegistry {
  
  final static public String FINISHED  = "finished";
  final static public String PROGRESS  = "progress";
  
  private Registry           registry;
  private String             registryPath;

  private Node               rootNode;
  
  private Node               trackingNode;
  private Node               trackingProgressNode;
  private Node               trackingFinishedNode;
  private Node               trackingLocksNode;

  private Node               windowNode;
  private Node               windowCommitsNode;
  private SequenceIdTracker  windowIdTracker;
  
  private ProgressWindowTracker progressWindowIdTracker = new ProgressWindowTracker();
  
  public TrackingWindowRegistry(Registry registry, String registryPath) throws RegistryException {
    this.registry         = registry;
    this.registryPath     = registryPath;
    
    rootNode     = registry.get(registryPath);
    trackingNode = rootNode.getChild("tracking");
    trackingProgressNode    = trackingNode.getChild(PROGRESS);
    trackingFinishedNode    = trackingNode.getChild(FINISHED);
    trackingLocksNode        = trackingNode.getChild("locks");
    
    windowNode       = rootNode.getChild("window");
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
    transaction.create(trackingFinishedNode, new  TrackingWindowReport("tracking"), NodeCreateMode.PERSISTENT);
    transaction.create(trackingLocksNode, null, NodeCreateMode.PERSISTENT);
    
    transaction.create(windowNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(windowCommitsNode, null, NodeCreateMode.PERSISTENT);
    windowIdTracker.initRegistry(transaction);
  }
  
  public int nextWindowId() throws RegistryException { return windowIdTracker.nextInt(); }
  
  public List<String> getProgressCommitWindows() throws RegistryException {
    return windowCommitsNode.getChildren();
  }
  
  public void saveWindow(TrackingWindow window) throws RegistryException {
    windowCommitsNode.createChild(window.toWindowIdName(), window, NodeCreateMode.PERSISTENT);
  }
  
  public void saveProgress(final TrackingWindowStat windowStat) throws RegistryException {
    String idName        = windowStat.toWindowIdName();
    final Node windowNode = trackingProgressNode.getChild(idName);
    
    if(!progressWindowIdTracker.isCreated(windowStat.getName(), windowStat.getWindowId())) {
      BatchOperations<Boolean> saveProgressOp = new BatchOperations<Boolean>() {
        @Override
        public Boolean execute(Registry registry) throws RegistryException {
          Transaction transaction = registry.getTransaction();
          if(!windowNode.exists()) {
            transaction.create(windowNode, windowStat, NodeCreateMode.PERSISTENT);
          } else {
            transaction.createChild(windowNode, "", windowStat, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
          }
          transaction.commit();
          return true;
        }
      };
      String lockMessage = 
          "Lock to create the window progress for name = " + windowStat.getName() + 
          ", maxWindowSize = " + windowStat.getMaxWindowSize() + 
          ", window id = " + windowStat.getWindowId() + ", thread = " + Thread.currentThread().getId();
      Lock lock = trackingLocksNode.getLock("write", lockMessage) ;
      lock.setDebug(true);
      lock.execute(saveProgressOp, 3, 5000);
      progressWindowIdTracker.create(windowStat.getName(), windowStat.getWindowId());
    } else {
      BatchOperations<Boolean> saveProgressOp = new BatchOperations<Boolean>() {
        @Override
        public Boolean execute(Registry registry) throws RegistryException {
          Transaction transaction = registry.getTransaction();
          transaction.createChild(windowNode, "", windowStat, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
          transaction.commit();
          return true;
        }
      };
      registry.executeBatch(saveProgressOp, 3, 3000);
    }
  }
  
  public TrackingWindowStat mergeProgress(final String windowIdName) throws RegistryException {
    BatchOperations<TrackingWindowStat> op = new BatchOperations<TrackingWindowStat>() {
      @Override
      public TrackingWindowStat execute(Registry registry) throws RegistryException {
        Node   progressWindowNode = trackingProgressNode.getChild(windowIdName);
        TrackingWindowStat mergeWindowStat = progressWindowNode.getDataAs(TrackingWindowStat.class);
       
        Node windowCommitNode = windowCommitsNode.getChild(windowIdName);
        TrackingWindow window = windowCommitNode.getDataAsWithDefault(TrackingWindow.class, null);
        if(window != null) {
          mergeWindowStat.setWindowSize(window.getWindowSize());
        }

        List<String> progressUpdates = progressWindowNode.getChildren();
        if(progressUpdates.size() > 0) {
          for(int i = 0; i < progressUpdates.size(); i++) {
            Node windowUpdateNode = progressWindowNode.getChild(progressUpdates.get(i));
            TrackingWindowStat updateWindowStat = windowUpdateNode.getDataAs(TrackingWindowStat.class);
            mergeWindowStat.merge(updateWindowStat);
          }
        } else {
          mergeWindowStat.update();
        }
        
        Transaction transaction = registry.getTransaction();
        if(mergeWindowStat.isComplete() && window != null) {
          Node   finishedWindowNode = trackingFinishedNode.getChild(windowIdName);
          transaction.create(finishedWindowNode, mergeWindowStat, NodeCreateMode.PERSISTENT);
          for(int i = 0; i < progressUpdates.size(); i++) {
            transaction.deleteChild(progressWindowNode, progressUpdates.get(i));
          }
          transaction.delete(progressWindowNode.getPath());
        } else {
          transaction.setData(progressWindowNode, mergeWindowStat);
          for(int i = 0; i < progressUpdates.size(); i++) {
            transaction.deleteChild(progressWindowNode, progressUpdates.get(i));
          }
        }
        transaction.commit();
        return mergeWindowStat;
      }
    };
    return registry.executeBatch(op, 3, 3000);
  }
  
  public TrackingWindowReport mergeProgress() throws RegistryException {
    List<String> progressWindows = trackingProgressNode.getChildren();
    TrackingWindowReport windowReport = new TrackingWindowReport("progress");
    for(int i = 0 ; i < progressWindows.size(); i++) {
      String windowIdName = progressWindows.get(i);
      TrackingWindowStat windowStat = mergeProgress(windowIdName);
      windowReport.mergeProgress(windowStat);
    }
    return windowReport;
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
    return trackingFinishedNode.getDataAs(TrackingWindowReport.class);
  }
  
  public TrackingWindowReport merge() throws RegistryException {
    TrackingWindowReport report = mergeFinished();
    TrackingWindowReport progress = mergeProgress();
    report.setProgressWindowStats(progress.getProgressWindowStats());
    return report;
  }
  
  @SuppressWarnings("serial")
  static public class ProgressWindowTracker extends LinkedHashMap<String, String> {
    private static final int MAX_ENTRIES = 1000;
    
    public void create(String name, int windowId) {
      String key = name + ":" + windowId;
      put(key, key);
    }
    
    public boolean isCreated(String name, int windowId) {
      String key = name + ":" + windowId;
      return containsKey(key);
    }

    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
       return size() > MAX_ENTRIES;
    }
  }
}
