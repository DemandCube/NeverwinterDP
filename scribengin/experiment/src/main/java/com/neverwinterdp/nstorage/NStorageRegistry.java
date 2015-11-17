package com.neverwinterdp.nstorage;

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

public class NStorageRegistry<T> {
  final static public String PARTITIONS   = "partitions";
  final static public String LOCKS        = "locks";
  final static public String CURSORS_READ = "cursors/read";
  
  private Registry          registry;
  private Node              nstorageNode;
  private Node              partitionsNode;
  private Node              locksNode;
  private Node              cursorsReadNode;
  private NStorageConfig<T> nstorageConfig;

  public NStorageRegistry(Registry registry, String path) throws RegistryException {
    this.registry = registry;
    nstorageNode = registry.get(path);
    partitionsNode = nstorageNode.getChild(PARTITIONS);
    locksNode = nstorageNode.getChild(LOCKS);
    cursorsReadNode = nstorageNode.getDescendant(CURSORS_READ);
    nstorageConfig = (NStorageConfig<T>) nstorageNode.getDataAs(NStorageConfig.class);
  }
  
  public NStorageRegistry(Registry registry, NStorageConfig<T> hqueue) throws RegistryException {
    this.registry = registry;
    if(registry.exists(hqueue.getRegistryPath())) {
      throw new RegistryException(ErrorCode.NodeExists, "The path " + hqueue.getRegistryPath() + " is already existed");
    }
    nstorageNode = registry.createIfNotExist(hqueue.getRegistryPath()) ;
    nstorageNode.setData(hqueue);
    
    partitionsNode = nstorageNode.createChild(PARTITIONS, NodeCreateMode.PERSISTENT);
    locksNode = nstorageNode.createChild(LOCKS, NodeCreateMode.PERSISTENT);
    cursorsReadNode = nstorageNode.createDescendantIfNotExists(CURSORS_READ);
    this.nstorageConfig = hqueue ;
    resizePartitions();
  }
  
  public NStorageConfig<T> getNStorageConfig() { return nstorageConfig; }
  
  public void resizePartitions() throws RegistryException {
    BatchOperations<Boolean> newPartitionOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Transaction transaction = registry.getTransaction();
          List<String> pNames = partitionsNode.getChildren();
          for(int i = pNames.size(); i < nstorageConfig.getNumOfPartition(); i++) {
            NStoragePartition partition = new NStoragePartition();
            partition.setPartitionId(i);
            String pName = "partition-" + partition.getPartitionId();
            partition.setRegistryLocation(partitionsNode.getPath() + "/" + pName);
            partition.setFsLocation(nstorageConfig.getFsLocation() + "/partitions/" + pName);
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
  
  public NStoragePartition getPartition(int partitionId) throws RegistryException {
    return partitionsNode.getChild("partition-" + partitionId).getDataAs(NStoragePartition.class) ;
  }
  
  public List<NStoragePartition> getPartitions() throws RegistryException {
    return partitionsNode.getChildrenAs(NStoragePartition.class) ;
  }
  
  public NStoragePartitionSegment newNStoragePartitionSegment(NStoragePartition partition) throws RegistryException {
    final NStoragePartitionSegment segment = new NStoragePartitionSegment();
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
  
  public List<NStoragePartitionSegment> getNStoragePartitionSegments(NStoragePartition partition) throws RegistryException {
    Node partitionNode = partitionsNode.getChild("partition-" + partition.getPartitionId()) ;
    Node segmentsNode = partitionNode.getChild("segments");
    List<NStoragePartitionSegment> segments = segmentsNode.getChildrenAs(NStoragePartitionSegment.class);
    Collections.sort(segments, NStoragePartitionSegment.ID_COMPARATOR);
    return segments;
  }
  
  public NStorageCursorRead getCursorRead(String name, boolean create) throws RegistryException {
    if(cursorsReadNode.hasChild(name)) {
      return cursorsReadNode.getChild(name).getDataAs(NStorageCursorRead.class);
    }
    NStorageCursorRead cursor = new NStorageCursorRead(name);
    cursorsReadNode.createChild(name, cursor, NodeCreateMode.PERSISTENT);
    return cursor ;
  }
  
  public NStoragePartitionCursorRead getPartitionCursorRead(NStorageCursorRead cursor, NStoragePartition partition) throws RegistryException {
    Node cursorNode = cursorsReadNode.getChild(cursor.getName());
    cursorNode.getChild("partitions").getChild("partition-" + partition.getPartitionId());
    NStoragePartitionCursorRead partitionCursor = cursorNode.getDataAs(NStoragePartitionCursorRead.class);
    return partitionCursor ;
  }
}
