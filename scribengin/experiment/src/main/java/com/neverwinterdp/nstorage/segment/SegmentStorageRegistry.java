package com.neverwinterdp.nstorage.segment;

import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;

public class SegmentStorageRegistry {
  private Registry  registry;
  private String    storagePath;
  private Node      storageNode;
  private Node      segmentsNode ;
  private Node      readersNode ;
  private Node      writersNode ;
  private Node      actionQueueNode;
  private Node      lockNode;
  
  public SegmentStorageRegistry(Registry registry, String path) throws RegistryException {
    this.registry = registry;
    this.storagePath = path;
    
    storageNode     = registry.get(storagePath);
    segmentsNode    = storageNode.getChild("segments");
    readersNode     = storageNode.getChild("readers");
    writersNode     = storageNode.getChild("writers");
    actionQueueNode = storageNode.getChild("action-queue");
    lockNode        = storageNode.getChild("lock");
  }
  
  public void initRegistry() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    initRegistry(transaction);
    transaction.commit();
  }
  
  public void initRegistry(Transaction trans) throws RegistryException {
    trans.create(storageNode,     null, NodeCreateMode.PERSISTENT);
    trans.create(segmentsNode,    null, NodeCreateMode.PERSISTENT);
    trans.create(readersNode,     null, NodeCreateMode.PERSISTENT);
    trans.create(writersNode,     null, NodeCreateMode.PERSISTENT);
    trans.create(actionQueueNode, null, NodeCreateMode.PERSISTENT);
    trans.create(lockNode,        null, NodeCreateMode.PERSISTENT);
  }
  
  public String getStoragePath() { return storagePath ; }
  
  public List<String> getSegments() throws RegistryException {
    return segmentsNode.getChildren() ;
  }
  
  public SegmentDescriptor getSegmentById(int id) throws RegistryException {
    return segmentsNode.getChild(SegmentDescriptor.toSegmentName(id)).getDataAs(SegmentDescriptor.class);
  }
  
  public SegmentDescriptor getSegmentByName(String name) throws RegistryException {
    return segmentsNode.getChild(name).getDataAs(SegmentDescriptor.class);
  }
  
  public SegmentDescriptor newSegment() throws RegistryException {
    BatchOperations<SegmentDescriptor> op = new BatchOperations<SegmentDescriptor>() {
      @Override
      public SegmentDescriptor execute(Registry registry) throws RegistryException {
        List<String> segments = segmentsNode.getChildren();
        SegmentDescriptor segment = new SegmentDescriptor(segments.size());
        Transaction transaction = registry.getTransaction();
        transaction.createChild(segmentsNode, segment.getName(), segment, NodeCreateMode.PERSISTENT);
        transaction.createDescendant(segmentsNode, segment.getName() + "/lock", NodeCreateMode.PERSISTENT) ;
        transaction.createDescendant(segmentsNode, segment.getName() + "/data", NodeCreateMode.PERSISTENT) ;
        transaction.commit();
        return segment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to create a new segment") ;
    return lock.execute(op, 3, 3000);
  }
  
  public void commit(final SegmentDescriptor segment) throws RegistryException {
    BatchOperations<SegmentDescriptor> op = new BatchOperations<SegmentDescriptor>() {
      @Override
      public SegmentDescriptor execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        Node segNode = segmentsNode.getChild(segment.getName());
        transaction.setData(segNode.getPath(), segment);
        transaction.commit();
        return segment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to update the segment " + segment.getName()) ;
    lock.execute(op, 3, 3000);
  }
  
  public DataSegmentDescriptor getDataSegmentById(SegmentDescriptor segment, int id) throws RegistryException {
    Node segNode     = segmentsNode.getChild(segment.getName());
    Node dataNode    = segNode.getChild("data");
    Node segDataNode = dataNode.getChild(DataSegmentDescriptor.toName(id));
    return segDataNode.getDataAs(DataSegmentDescriptor.class);
  }
  
  public List<String> getDataSegments(SegmentDescriptor segment) throws RegistryException {
    Node segNode     = segmentsNode.getChild(segment.getName());
    Node segDataNode = segNode.getChild("data");
    return segDataNode.getChildren();
  }
  
  public DataSegmentDescriptor newDataSegment(final String creator, final SegmentDescriptor segment) throws RegistryException {
    BatchOperations<DataSegmentDescriptor> op = new BatchOperations<DataSegmentDescriptor>() {
      @Override
      public DataSegmentDescriptor execute(Registry registry) throws RegistryException {
        Node segNode     = segmentsNode.getChild(segment.getName());
        Node segDataNode = segNode.getChild("data");
        List<String> dataSegments = segDataNode.getChildren();
        DataSegmentDescriptor dataSegment = 
          new DataSegmentDescriptor(dataSegments.size(), creator);
        Transaction transaction = registry.getTransaction();
        transaction.createChild(segDataNode, dataSegment.getName(), dataSegment, NodeCreateMode.PERSISTENT);
        transaction.commit();
        return dataSegment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to create a new segment") ;
    return lock.execute(op, 3, 3000);
  }
  
  public DataSegmentDescriptor commit(final SegmentDescriptor segment, final DataSegmentDescriptor dataSeg) throws RegistryException {
    final Node segNode     = segmentsNode.getChild(segment.getName());
    final Node dataNode    = segNode.getChild("data");
    final Node segDataNode = dataNode.getChild(dataSeg.getName());
    BatchOperations<DataSegmentDescriptor> op = new BatchOperations<DataSegmentDescriptor>() {
      @Override
      public DataSegmentDescriptor execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        transaction.setData(segDataNode, dataSeg);
        transaction.commit();
        return dataSeg;
      }
    };
    Node lockNode = segNode.getChild("lock");
    Lock lock = lockNode.getLock("write", "Lock to update a a data segment") ;
    return lock.execute(op, 3, 3000);
  }
}