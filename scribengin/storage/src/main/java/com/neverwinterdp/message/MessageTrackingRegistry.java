package com.neverwinterdp.message;

import java.util.Collections;
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
    return messageChunkIdTracker.nextInt(); 
  }
  
  public void saveMessageTrackingChunkStat(MessageTrackingChunkStat chunk) throws RegistryException {
    final Node reportNode = trackingLogNode.getChild(chunk.getName());
    if(!reportNode.exists()) {
      BatchOperations<Boolean> op = new BatchOperations<Boolean>() {
        @Override
        public Boolean execute(Registry registry) throws RegistryException {
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
    }
    
    if(chunk.isComplete()) saveComplete(reportNode, chunk) ;
    else saveProgress(reportNode, chunk) ;
  }
  
  void saveComplete(final Node reportNode, final MessageTrackingChunkStat chunk) throws RegistryException {
    BatchOperations<Boolean> saveCompleteOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        String idName = chunk.toChunkIdName();
        if(chunk.isPersisted()) {
          Node progressNode = reportNode.getChild(PROGRESS);
          transaction.deleteChild(progressNode, idName);
        } 
        Node finishedNode = reportNode.getChild(FINISHED);
        transaction.createChild(finishedNode, idName, chunk, NodeCreateMode.PERSISTENT);
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveCompleteOp, 3, 3000);
  }
  
  void saveProgress(final Node reportNode, final MessageTrackingChunkStat chunk) throws RegistryException {
    BatchOperations<Boolean> saveCompleteOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        String idName = chunk.toChunkIdName();
        Node progressNode = reportNode.getChild(PROGRESS);
        Node chunkNode = progressNode.getChild(idName);
        if(chunk.isPersisted()) {
          transaction.setData(chunkNode, chunk);
        } else {
          transaction.create(chunkNode, chunk, NodeCreateMode.PERSISTENT);
          chunk.setPersisted(true);
        }
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(saveCompleteOp, 3, 3000);
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
