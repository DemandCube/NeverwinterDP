package com.neverwinterdp.message;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.message.WindowId.Tracker;
import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;

public class MessageTrackingRegistry {
  
  final static public String FINISHED  = "finished";
  final static public String PROGRESS  = "progress";
  
  private Registry           registry;
  private String             registryPath;

  private Node               rootNode;
  
  private Node               trackingNode;
  private Node               locksNode;

  private Node               windowIdNode;
  private SequenceIdTracker  windowIdTracker;
  
  private HashSet<String>      trackingNames           = new HashSet<>();
  private ProgressWindowTracker progressWindowIdTracker = new ProgressWindowTracker();
  
  public MessageTrackingRegistry(Registry registry, String registryPath) throws RegistryException {
    this.registry         = registry;
    this.registryPath     = registryPath;
    
    rootNode     = registry.get(registryPath);
    trackingNode = rootNode.getChild("tracking");
    locksNode    = rootNode.getChild("locks");
    
    windowIdNode    = rootNode.getChild("window-id");
    windowIdTracker= new SequenceIdTracker(registry, windowIdNode.getPath() + "/tracker", false);
  }
  
  public void initRegistry() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    initRegistry(transaction);
    transaction.commit();
  }
  
  public void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(rootNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(trackingNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(locksNode, null, NodeCreateMode.PERSISTENT);
    
    transaction.create(windowIdNode, new WindowId(), NodeCreateMode.PERSISTENT);
    windowIdTracker.initRegistry(transaction);
  }
  
  public int nextWindowId() throws RegistryException { 
    return windowIdTracker.nextInt();
  }
  
  public int nextWindowId(String checkCompleteWithTracker, int windowIdLimit) throws RegistryException { 
    WindowId windowId = windowIdNode.getDataAs(WindowId.class);
    int completeWindowCount = 0;
    Tracker tracker = windowId.getTracker(checkCompleteWithTracker, false);
    if(tracker != null) {
      completeWindowCount = tracker.getCompleteWindowCount();
    }
    
    int currentWindowId = windowIdTracker.currentInt();
    if(currentWindowId - completeWindowCount < windowIdLimit)  {
      int retWindowId = windowIdTracker.nextInt();
      //System.err.println("currentWindowId = " + currentWindowId + ", completeWindowCount = " + completeWindowCount +", windowId = " + retWindowId);
      return retWindowId;
    }
    //System.err.println("currentWindowId = " + currentWindowId + ", completeWindowCount = " + completeWindowCount +", windowId = -1");
    return -1;
  }
  
  public WindowMessageTrackingStat getProgress(final String name, final int windowId) throws RegistryException {
    Node reportNode   = trackingNode.getChild(name);
    Node progressNode = reportNode.getChild(PROGRESS);
    Node windowNode    = progressNode.getChild(WindowMessageTrackingStat.toWindowIdName(windowId));
    WindowMessageTrackingStat mergeWindow = windowNode.getDataAs(WindowMessageTrackingStat.class);
    return mergeWindow;
  }
  
  public WindowMessageTrackingStat getFinished(final String name, final int windowId) throws RegistryException {
    Node reportNode   = trackingNode.getChild(name);
    Node finishedNode = reportNode.getChild(FINISHED);
    Node windowNode    = finishedNode.getChild(WindowMessageTrackingStat.toWindowIdName(windowId));
    WindowMessageTrackingStat mergeWindow = windowNode.getDataAs(WindowMessageTrackingStat.class);
    return mergeWindow;
  }
  
  public void saveProgress(final WindowMessageTrackingStat windowStat) throws RegistryException {
    //check and create tracking structure according to the save point name
    if(!trackingNames.contains(windowStat.getName())) {
      BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
        @Override
        public Boolean execute(Registry registry) throws RegistryException {
          Node reportNode = trackingNode.getChild(windowStat.getName());
          if(reportNode.exists()) return true;
          
          Transaction transaction = registry.getTransaction();
          transaction.create(reportNode, null, NodeCreateMode.PERSISTENT);
          Node progressNode = reportNode.getChild(PROGRESS);
          transaction.create(progressNode, null, NodeCreateMode.PERSISTENT);
          Node finishedNode = reportNode.getChild(FINISHED);
          transaction.create(finishedNode, null, NodeCreateMode.PERSISTENT);
          transaction.commit();
          
          return true;
        }
      };
      Lock lock = locksNode.getLock("write", "Lock to Create Report structure") ;
      lock.execute(op, 3, 3000);
      trackingNames.add(windowStat.getName());
    }
    
    Node reportNode      = trackingNode.getChild(windowStat.getName());
    Node progressNode    = reportNode.getChild(PROGRESS);
    String idName        = windowStat.toWindowIdName();
    final Node windowNode = progressNode.getChild(idName);
    
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
      Lock lock = locksNode.getLock("write", "Lock to create the window progress for window size = " + windowStat.getWindowSize() + ", window id = " + windowStat.getWindowId()) ;
      lock.execute(saveProgressOp, 3, 3000);
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
  
  public WindowMessageTrackingStat mergeProgress(final String name, final int windowId) throws RegistryException {
    final String idName  = WindowMessageTrackingStat.toWindowIdName(windowId);
    return mergeProgress(name, idName);
  }
  
  public WindowMessageTrackingStat mergeProgress(final String name, final String idName) throws RegistryException {
    BatchOperations<WindowMessageTrackingStat> op = new BatchOperations<WindowMessageTrackingStat>() {
      @Override
      public WindowMessageTrackingStat execute(Registry registry) throws RegistryException {
        Node   reportNode   = trackingNode.getChild(name);
        Node   progressNode = reportNode.getChild(PROGRESS);
        
        Node   progressChunkNode = progressNode.getChild(idName);
        WindowMessageTrackingStat mergeChunk = progressChunkNode.getDataAs(WindowMessageTrackingStat.class);
        List<String> progressUpdates = progressChunkNode.getChildren();
        if(progressUpdates.size() > 0) {
          for(int i = 0; i < progressUpdates.size(); i++) {
            Node windowUpdateNode = progressChunkNode.getChild(progressUpdates.get(i));
            WindowMessageTrackingStat updateChunk = windowUpdateNode.getDataAs(WindowMessageTrackingStat.class);
            mergeChunk.merge(updateChunk);
          }
        } else {
          mergeChunk.update();
        }
        Transaction transaction = registry.getTransaction();
        if(mergeChunk.isComplete()) {
          Node   finishedNode = reportNode.getChild(FINISHED);
          Node   finishedChunkNode = finishedNode.getChild(idName);
          transaction.create(finishedChunkNode, mergeChunk, NodeCreateMode.PERSISTENT);
          for(int i = 0; i < progressUpdates.size(); i++) {
            transaction.deleteChild(progressChunkNode, progressUpdates.get(i));
          }
          transaction.delete(progressChunkNode.getPath());
        } else {
          transaction.setData(progressChunkNode, mergeChunk);
          for(int i = 0; i < progressUpdates.size(); i++) {
            transaction.deleteChild(progressChunkNode, progressUpdates.get(i));
          }
        }
        transaction.commit();
        return mergeChunk;
      }
    };
    return registry.executeBatch(op, 3, 3000);
  }
  
  public void mergeProgress(final String name) throws RegistryException {
    Node   reportNode   = trackingNode.getChild(name);
    Node   progressNode = reportNode.getChild(PROGRESS);
    if(!progressNode.exists()) return;
    List<String> progressWindows = progressNode.getChildren();
    for(int i = 0 ; i < progressWindows.size(); i++) {
      String progressChunk = progressWindows.get(i);
      mergeProgress(name, progressChunk);
    }
  }

  public MessageTrackingReport getMessageTrackingReporter(String name) throws RegistryException {
    MessageTrackingReport reporter = new MessageTrackingReport(name);
    Node reportNode = trackingNode.getChild(name);
    Node finishedNode = reportNode.getChild(FINISHED);
    MessageTrackingReport finishedReporter = null; 
    if(finishedNode.exists()) finishedReporter = finishedNode.getDataAs(MessageTrackingReport.class);
    if(finishedReporter == null) finishedReporter = new  MessageTrackingReport(name);
    MessageTrackingReport progressReporter = getProgressReport(name);
    reporter.setFinishedWindowStats(finishedReporter.getFinishedWindowStats());
    reporter.setProgressWindowStats(progressReporter.getProgressWindowStats());
    return reporter;
  }
  
  public MessageTrackingReport mergeReport(String name) throws RegistryException {
    MessageTrackingReport report = new MessageTrackingReport(name);
    MessageTrackingReport progressReport = getProgressReport(name);
    MessageTrackingReport finishedReport = mergeFinishedReport(name);
    report.setFinishedWindowStats(finishedReport.getFinishedWindowStats());
    report.setProgressWindowStats(progressReport.getProgressWindowStats());
    return report;
  }
  
  public MessageTrackingReport mergeFinishedReport(final String name) throws RegistryException {
    final Node reportNode = trackingNode.getChild(name);
    final Node finishedNode = reportNode.getChild(FINISHED);
    if(!finishedNode.exists()) return new  MessageTrackingReport(name);

    MessageTrackingReport reporter = finishedNode.getDataAs(MessageTrackingReport.class);
    if(reporter == null) reporter = new  MessageTrackingReport(name);
    
    final List<String> finishedWindows = finishedNode.getChildren();
    Collections.sort(finishedWindows);
    for(int i= 0; i < finishedWindows.size(); i++) {
      Node windowNode = finishedNode.getChild(finishedWindows.get(i));
      WindowMessageTrackingStat windowStat = windowNode.getDataAs( WindowMessageTrackingStat.class);
      reporter.mergeFinished(windowStat);
    }
    reporter.optimize();
    
    final MessageTrackingReport reporterToSave = reporter;
    BatchOperations<Boolean> saveCompleteOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        transaction.setData(finishedNode, reporterToSave);
        for(int i= 0; i < finishedWindows.size(); i++) {
          transaction.deleteChild(finishedNode, finishedWindows.get(i));
        }
        
        WindowId windowId = windowIdNode.getDataAs(WindowId.class);
        Tracker tracker = windowId.getTracker(name, true);
        tracker.setCompleteWindowCount(reporterToSave.getCompleteWindowCount());
        transaction.setData(windowIdNode, windowId);
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveCompleteOp, 3, 3000);
    return reporter;
  }
  
  public MessageTrackingReport getProgressReport(String name) throws RegistryException {
    final Node reportNode = trackingNode.getChild(name);
    final Node progressNode = reportNode.getChild(PROGRESS);
    if(!progressNode.exists()) return new  MessageTrackingReport(name);
    
    MessageTrackingReport reporter  = new  MessageTrackingReport(name);
    
    final List<String> progressChunks = progressNode.getChildren();
    Collections.sort(progressChunks);
    for(int i= 0; i < progressChunks.size(); i++) {
      Node windowNode = progressNode.getChild(progressChunks.get(i));
      WindowMessageTrackingStat windowStat = windowNode.getDataAs( WindowMessageTrackingStat.class);
      reporter.mergeProgress(windowStat);
    }
    reporter.optimize();
    return reporter;
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
