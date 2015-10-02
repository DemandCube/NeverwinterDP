package com.neverwinterdp.hqueue;

import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;

public class HQueueRegistry<T> {
  final static public String PARTITIONS   = "partitions";
  final static public String LOCKS        = "locks";
  final static public String CURSORS_READ = "cursors/read";
  
  private Registry registry ;
  private Node queueNode ;
  private Node partitionsNode ;
  private Node locksNode ;
  private Node cursorsReadNode ;
  private HQueue<T> hqueue ;

  public HQueueRegistry(Registry registry, String path) throws RegistryException {
    this.registry = registry;
    queueNode = registry.get(path);
    partitionsNode = queueNode.getChild(PARTITIONS);
    locksNode = queueNode.getChild(LOCKS);
    cursorsReadNode = queueNode.getDescendant(CURSORS_READ);
    hqueue = (HQueue<T>) queueNode.getDataAs(HQueue.class);
  }
  
  public HQueueRegistry(Registry registry, HQueue<T> hqueue) throws RegistryException {
    this.registry = registry;
    if(registry.exists(hqueue.getRegistryPath())) {
      throw new RegistryException(ErrorCode.NodeExists, "The path " + hqueue.getRegistryPath() + " is already existed");
    }
    queueNode = registry.createIfNotExist(hqueue.getRegistryPath()) ;
    queueNode.setData(hqueue);
    
    partitionsNode = queueNode.createChild(PARTITIONS, NodeCreateMode.PERSISTENT);
    locksNode = queueNode.createChild(LOCKS, NodeCreateMode.PERSISTENT);
    cursorsReadNode = queueNode.createDescendantIfNotExists(CURSORS_READ);
    this.hqueue = hqueue ;
    resizePartitions();
  }
  
  public HQueue<T> getHQueue() { return hqueue; }
  
  public void resizePartitions() throws RegistryException {
    BatchOperations<Boolean> newPartitionOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Transaction transaction = registry.getTransaction();
          List<String> pNames = partitionsNode.getChildren();
          for(int i = pNames.size(); i < hqueue.getNumOfPartition(); i++) {
            HQueuePartition partition = new HQueuePartition();
            partition.setPartitionId(i);
            String pName = "partition-" + partition.getPartitionId();
            partition.setRegistryLocation(partitionsNode.getPath() + "/" + pName);
            partition.setFsLocation(hqueue.getFsLocation() + "/partitions/" + pName);
            transaction.createChild(partitionsNode, pName, partition, NodeCreateMode.PERSISTENT);
          }
          transaction.commit();
          return true;
        } catch(RegistryException ex) {
          throw ex;
        }
      }
    };
    Lock lock = locksNode.getLock("write", "Locl to create a new partition") ;
    lock.execute(newPartitionOp, 3, 3000);
  }
  
  public HQueuePartition getPartition(int partitionId) throws RegistryException {
    return partitionsNode.getChild("partition-" + partitionId).getDataAs(HQueuePartition.class) ;
  }
  
  public List<HQueuePartition> getPartitions() throws RegistryException {
    return partitionsNode.getChildrenAs(HQueuePartition.class) ;
  }
  
  public HQueuePartitionSegment newHQueuePartitionSegment(HQueuePartition partition) throws RegistryException {
    final HQueuePartitionSegment segment = new HQueuePartitionSegment();
    BatchOperations<Boolean> newPartitionOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Transaction transaction = registry.getTransaction();
          transaction.commit();
          return true;
        } catch(RegistryException ex) {
          throw ex;
        }
      }
    };
    Lock lock = locksNode.getLock("write", "Locl to create a new partition") ;
    lock.execute(newPartitionOp, 3, 3000);
    return segment;
  }
  
  public List<HQueuePartitionSegment> getHQueuePartitionSegments(HQueuePartition partition) throws RegistryException {
    Node partitionNode = partitionsNode.getChild("partition-" + partition.getPartitionId()) ;
    Node segmentsNode = partitionNode.getChild("segments");
    List<HQueuePartitionSegment> segments = segmentsNode.getChildrenAs(HQueuePartitionSegment.class);
    Collections.sort(segments, HQueuePartitionSegment.ID_COMPARATOR);
    return segments;
  }
  
  public HQueueCursorRead getHQueueCursorRead(String name, boolean create) throws RegistryException {
    if(cursorsReadNode.hasChild(name)) {
      return cursorsReadNode.getChild(name).getDataAs(HQueueCursorRead.class);
    }
    HQueueCursorRead cursor = new HQueueCursorRead(name);
    cursorsReadNode.createChild(name, cursor, NodeCreateMode.PERSISTENT);
    return cursor ;
  }
  
  public HQueueCursorReadPartition getHQueueCursorRead(HQueueCursorRead cursor, HQueuePartition partition) throws RegistryException {
    Node cursorNode = cursorsReadNode.getChild(cursor.getName());
    cursorNode.getChild("partitions").getChild("partition-" + partition.getPartitionId());
    HQueueCursorReadPartition partitionCursor = 
      cursorNode.getDataAs(HQueueCursorReadPartition.class);
    return partitionCursor ;
  }
}
