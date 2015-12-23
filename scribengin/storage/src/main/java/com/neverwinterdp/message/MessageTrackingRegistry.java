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
    String idName = MessageTrackingChunkStat.toChunkIdName(chunkId);
    Node chunkNode = progressNode.getChild(idName);
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
    BatchOperations<MessageTrackingChunkStat> op = new BatchOperations<MessageTrackingChunkStat>() {
      @Override
      public MessageTrackingChunkStat execute(Registry registry) throws RegistryException {
        Node   reportNode   = trackingLogNode.getChild(name);
        Node   progressNode = reportNode.getChild(PROGRESS);
        String idName       = MessageTrackingChunkStat.toChunkIdName(chunkId);
        Node   chunkNode    = progressNode.getChild(idName);
        MessageTrackingChunkStat mergeChunk = chunkNode.getDataAs(MessageTrackingChunkStat.class);
        List<String> updates = chunkNode.getChildren();
        for(int i = 0; i < updates.size(); i++) {
          Node chunkUpdateNode = chunkNode.getChild(updates.get(i));
          MessageTrackingChunkStat updateChunk = chunkUpdateNode.getDataAs(MessageTrackingChunkStat.class);
          mergeChunk.merge(updateChunk);
        }
        Transaction transaction = registry.getTransaction();
        transaction.setData(chunkNode, mergeChunk);
        for(int i = 0; i < updates.size(); i++) {
          transaction.deleteChild(chunkNode, updates.get(i));
        }
        transaction.commit();
        return mergeChunk;
      }
    };
    return registry.executeBatch(op, 3, 3000);
  }
  
  public void saveMessageTrackingChunkStat(final MessageTrackingChunkStat chunk) throws RegistryException {
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
    BatchOperations<Boolean> saveProgressOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        Node progressNode = reportNode.getChild(PROGRESS);
        String idName     = chunk.toChunkIdName();
        Node   chunkNode  = progressNode.getChild(idName);
        
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
    registry.executeBatch(saveProgressOp, 3, 3000);
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
