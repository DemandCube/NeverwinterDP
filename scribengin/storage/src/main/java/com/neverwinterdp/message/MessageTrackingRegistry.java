package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;

public class MessageTrackingRegistry {
  final static public String FINISHED  = "finished";
  final static public String PROGRESS  = "progress";
  
  private Registry           registry;
  private String             registryPath;

  private Node               rootNode;
  private Node               trackingLogNode;
  private SequenceIdTracker  messageChunkIdTracker;
  
  public MessageTrackingRegistry(Registry registry, String registryPath) throws RegistryException {
    this.registry         = registry;
    this.registryPath     = registryPath;
    
    rootNode = registry.get(registryPath);
    trackingLogNode = rootNode.getChild("tracking-log");
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
    messageChunkIdTracker.initRegistry(transaction);
  }
  
  public int nextMessageChunkId() throws RegistryException { 
    return messageChunkIdTracker.nextInt(); 
  }
  
  public void saveMessageTrackingLogChunk(MessageTrackingLogChunk chunk) throws RegistryException {
    Node reportNode = trackingLogNode.getChild(chunk.getName());
    Node progressNode = reportNode.getChild(PROGRESS);
    if(!reportNode.exists()) {
      reportNode.create(NodeCreateMode.PERSISTENT);
      progressNode.create(NodeCreateMode.PERSISTENT);
      Node finishedNode = reportNode.getChild(FINISHED);
      finishedNode.create(NodeCreateMode.PERSISTENT);
    }
    progressNode.createChild(chunk.toChunkIdName(), chunk, NodeCreateMode.PERSISTENT);
  }
  
  public MessageTrackingLogReporter mergeMessageTrackingLogChunk(String name) throws RegistryException {
    MessageTrackingLogReporter reporter = new  MessageTrackingLogReporter(name);
    Node reportNode = trackingLogNode.getChild(reporter.getName());
    reportNode.setData(reporter);
    return reporter;
  }
}
