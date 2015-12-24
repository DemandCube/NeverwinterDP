package com.neverwinterdp.message;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
  private Node               trackingLogNode;
  private Node               locksNode;
  private SequenceIdTracker  messageChunkIdTracker;
  
  private HashSet<String>    createdTrackingNames = new HashSet<>();
  
  public MessageTrackingRegistry(Registry registry, String registryPath) throws RegistryException {
    this.registry         = registry;
    this.registryPath     = registryPath;
    
    rootNode = registry.get(registryPath);
    trackingLogNode = rootNode.getChild("tracking-log");
    locksNode        = rootNode.getChild("locks");
    messageChunkIdTracker = new SequenceIdTracker(registry, rootNode.getPath() + "/chunk-id-tracker", false);
  }
  
  public void initRegistry() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    initRegistry(transaction);
    transaction.commit();
  }
  
  public void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(rootNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(trackingLogNode, null, NodeCreateMode.PERSISTENT);
    transaction.create(locksNode, null, NodeCreateMode.PERSISTENT);
    messageChunkIdTracker.initRegistry(transaction);
  }
  
  public int nextMessageChunkId() throws RegistryException { 
    int chunkId = messageChunkIdTracker.nextInt(); 
    return chunkId;
  }
  
  public MessageTrackingChunkStat getProgress(final String name, final int chunkId) throws RegistryException {
    Node reportNode = trackingLogNode.getChild(name);
    Node progressNode = reportNode.getChild(PROGRESS);
    Node chunkNode = progressNode.getChild(MessageTrackingChunkStat.toChunkIdName(chunkId));
    MessageTrackingChunkStat mergeChunk = chunkNode.getDataAs(MessageTrackingChunkStat.class);
    return mergeChunk;
  }
  
  public MessageTrackingChunkStat getFinished(final String name, final int chunkId) throws RegistryException {
    Node reportNode = trackingLogNode.getChild(name);
    Node finishedNode = reportNode.getChild(FINISHED);
    Node chunkNode = finishedNode.getChild(MessageTrackingChunkStat.toChunkIdName(chunkId));
    MessageTrackingChunkStat mergeChunk = chunkNode.getDataAs(MessageTrackingChunkStat.class);
    return mergeChunk;
  }
  
  public void saveProgress(final MessageTrackingChunkStat chunk) throws RegistryException {
    //check and create tracking structure according to the save point name
    if(!createdTrackingNames.contains(chunk.getName())) {
      BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
        @Override
        public Boolean execute(Registry registry) throws RegistryException {
          Node reportNode = trackingLogNode.getChild(chunk.getName());
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
      createdTrackingNames.add(chunk.getName());
    }
    
    Node reportNode = trackingLogNode.getChild(chunk.getName());
    Node progressNode = reportNode.getChild(PROGRESS);
    String idName     = chunk.toChunkIdName();
    final Node chunkNode = progressNode.getChild(idName);
    
    BatchOperations<Boolean> saveProgressOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        if(!chunkNode.exists()) {
          transaction.create(chunkNode, chunk, NodeCreateMode.PERSISTENT);
        } else {
          transaction.createChild(chunkNode, "", chunk, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
        }
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveProgressOp, 3, 3000);
  }
  
  public MessageTrackingChunkStat mergeProgress(final String name, final int chunkId) throws RegistryException {
    final String idName  = MessageTrackingChunkStat.toChunkIdName(chunkId);
    return mergeProgress(name, idName);
  }
  
  public MessageTrackingChunkStat mergeProgress(final String name, final String idName) throws RegistryException {
    BatchOperations<MessageTrackingChunkStat> op = new BatchOperations<MessageTrackingChunkStat>() {
      @Override
      public MessageTrackingChunkStat execute(Registry registry) throws RegistryException {
        Node   reportNode   = trackingLogNode.getChild(name);
        Node   progressNode = reportNode.getChild(PROGRESS);
        
        Node   progressChunkNode = progressNode.getChild(idName);
        MessageTrackingChunkStat mergeChunk = progressChunkNode.getDataAs(MessageTrackingChunkStat.class);
        List<String> progressUpdates = progressChunkNode.getChildren();
        if(progressUpdates.size() > 0) {
          for(int i = 0; i < progressUpdates.size(); i++) {
            Node chunkUpdateNode = progressChunkNode.getChild(progressUpdates.get(i));
            MessageTrackingChunkStat updateChunk = chunkUpdateNode.getDataAs(MessageTrackingChunkStat.class);
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
    Node   reportNode   = trackingLogNode.getChild(name);
    Node   progressNode = reportNode.getChild(PROGRESS);
    List<String> progressChunks = progressNode.getChildren();
    for(int i = 0 ; i < progressChunks.size(); i++) {
      String progressChunk = progressChunks.get(i);
      mergeProgress(name, progressChunk);
    }
  }

  public MessageTrackingReporter mergeMessageTrackingLogChunk(String name) throws RegistryException {
    final Node reportNode = trackingLogNode.getChild(name);
    MessageTrackingReporter reporter = null;
    if(reportNode.exists()) {
      reporter = reportNode.getDataAs(MessageTrackingReporter.class);
    }
    if(reporter == null) reporter = new  MessageTrackingReporter(name);
    
    final Node finishedNode = reportNode.getChild(FINISHED);
    final List<String> finishedChunks = finishedNode.getChildren();
    Collections.sort(finishedChunks);
    for(int i= 0; i < finishedChunks.size(); i++) {
      Node chunkNode = finishedNode.getChild(finishedChunks.get(i));
      MessageTrackingChunkStat chunk = chunkNode.getDataAs( MessageTrackingChunkStat.class);
      reporter.merge(chunk);
    }
    reporter.optimize();
    
    final MessageTrackingReporter reporterToSave = reporter;
    BatchOperations<Boolean> saveCompleteOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        transaction.setData(reportNode, reporterToSave);
        for(int i= 0; i < finishedChunks.size(); i++) {
          transaction.deleteChild(finishedNode, finishedChunks.get(i));
        }
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveCompleteOp, 3, 3000);
    return reporter;
  }
}
