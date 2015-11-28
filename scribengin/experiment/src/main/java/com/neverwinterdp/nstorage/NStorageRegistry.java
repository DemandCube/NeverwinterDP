package com.neverwinterdp.nstorage;

import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;

public class NStorageRegistry {
  final static public String READERS           = "readers";
  final static public String READERS_ALL       = READERS + "/all";
  final static public String READERS_ACTIVE    = READERS + "/active";
  final static public String READERS_HISTORY   = READERS + "/history";
  final static public String READERS_HEARTBEAT = READERS + "/heartbeat";
  
  final static public String WRITERS           = "writers";
  final static public String WRITERS_ALL       = WRITERS + "/all";
  final static public String WRITERS_ACTIVE    = WRITERS + "/active";
  final static public String WRITERS_HEARTBEAT = WRITERS + "/heartbeat";
  final static public String WRITERS_HISTORY   = WRITERS + "/history";
  
  private Registry registry;
  private String   registryPath;

  private Node registryNode;
  private Node segmentsNode;

  private Node              readersNode;
  private Node              readersAllNode;
  private Node              readersActiveNode;
  private Node              readersHeartbeatNode;
  private Node              readersHistoryNode;
  private SequenceIdTracker readerIdTracker;

  private Node              writersNode;
  private Node              writersAllNode;
  private Node              writersActiveNode;
  private Node              writersHeartbeatNode;
  private Node              writersHistoryNode;
  private SequenceIdTracker writerIdTracker;
  
  private Node actionQueueNode;

  private Node      lockNode;
  
  public NStorageRegistry(Registry registry, String path) throws RegistryException {
    this.registry = registry;
    this.registryPath = path;
    
    registryNode       = registry.get(registryPath);
    segmentsNode       = registryNode.getChild("segments");
    
    readersNode          = registryNode.getChild(READERS);
    readersAllNode       = registryNode.getDescendant(READERS_ALL);
    readersActiveNode    = registryNode.getDescendant(READERS_ACTIVE);
    readersHeartbeatNode = registryNode.getDescendant(READERS_HEARTBEAT);
    readersHistoryNode   = registryNode.getDescendant(READERS_HISTORY);
    readerIdTracker      = new SequenceIdTracker(registry, readersNode.getPath() + "/id-tracker", false);
    
    writersNode          = registryNode.getChild(WRITERS);
    writersAllNode       = registryNode.getDescendant(WRITERS_ALL);
    writersActiveNode    = registryNode.getDescendant(WRITERS_ACTIVE);
    writersHeartbeatNode = registryNode.getDescendant(WRITERS_HEARTBEAT);
    writersHistoryNode   = registryNode.getDescendant(WRITERS_HISTORY);
    writerIdTracker      = new SequenceIdTracker(registry, writersNode.getPath() + "/id-tracker", false);
    
    actionQueueNode    = registryNode.getChild("action-queue");
    lockNode           = registryNode.getChild("lock");
  }
  
  public void initRegistry() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    initRegistry(transaction);
    transaction.commit();
  }
  
  public void initRegistry(Transaction trans) throws RegistryException {
    trans.create(registryNode,       null, NodeCreateMode.PERSISTENT);
    trans.create(segmentsNode,       null, NodeCreateMode.PERSISTENT);
    
    trans.create(readersNode,          null, NodeCreateMode.PERSISTENT);
    trans.create(readersAllNode,       null, NodeCreateMode.PERSISTENT);
    trans.create(readersActiveNode,    null, NodeCreateMode.PERSISTENT);
    trans.create(readersHeartbeatNode, null, NodeCreateMode.PERSISTENT);
    trans.create(readersHistoryNode,   null, NodeCreateMode.PERSISTENT);
    readerIdTracker.initRegistry(trans);
    
    trans.create(writersNode,          null, NodeCreateMode.PERSISTENT);
    trans.create(writersAllNode,       null, NodeCreateMode.PERSISTENT);
    trans.create(writersActiveNode,    null, NodeCreateMode.PERSISTENT);
    trans.create(writersHeartbeatNode, null, NodeCreateMode.PERSISTENT);
    trans.create(writersHistoryNode,   null, NodeCreateMode.PERSISTENT);
    writerIdTracker.initRegistry(trans);
    
    trans.create(actionQueueNode,    null, NodeCreateMode.PERSISTENT);
    trans.create(lockNode,           null, NodeCreateMode.PERSISTENT);
  }
  
  public String getRegistryPath() { return registryPath ; }
  
  public List<String> getSegments() throws RegistryException {
    return segmentsNode.getChildren() ;
  }
  
  public SegmentDescriptor getSegmentById(int id) throws RegistryException {
    return segmentsNode.getChild(SegmentDescriptor.toSegmentName(id)).getDataAs(SegmentDescriptor.class);
  }
  
  public SegmentDescriptor getSegmentByName(String name) throws RegistryException {
    return segmentsNode.getChild(name).getDataAs(SegmentDescriptor.class);
  }
  
  public SegmentDescriptor newSegment(final NStorageWriterDescriptor writer) throws RegistryException {
    BatchOperations<SegmentDescriptor> op = new BatchOperations<SegmentDescriptor>() {
      @Override
      public SegmentDescriptor execute(Registry registry) throws RegistryException {
        List<String> segments = segmentsNode.getChildren();
        SegmentDescriptor segment = new SegmentDescriptor(segments.size());
        segment.setCreator(writer.getWriter());
        Transaction transaction = registry.getTransaction();
        transaction.createChild(segmentsNode, segment.getName(), segment, NodeCreateMode.PERSISTENT);
        transaction.createDescendant(segmentsNode, segment.getName() + "/lock", NodeCreateMode.PERSISTENT) ;
        transaction.createDescendant(segmentsNode, segment.getName() + "/data", NodeCreateMode.PERSISTENT) ;
        
        writer.logStartSegment(segment.getName());
        transaction.setData(writersAllNode.getPath() + "/" + writer.getId(), writer);
        transaction.commit();
        return segment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to create a new segment") ;
    return lock.execute(op, 3, 3000);
  }
  
  public void commit(NStorageWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException {
    save(writer, segment, false);
  }
  
  public void finish(NStorageWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException {
    save(writer, segment, true);
  }
  
  void save(final NStorageWriterDescriptor writer, final SegmentDescriptor segment, final boolean finished) throws RegistryException {
    BatchOperations<SegmentDescriptor> op = new BatchOperations<SegmentDescriptor>() {
      @Override
      public SegmentDescriptor execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        Node segNode = segmentsNode.getChild(segment.getName());
        transaction.setData(segNode.getPath(), segment);
        if(finished) {
          writer.logFinishSegment();
          transaction.setData(writersAllNode.getPath() + "/" + writer.getId(), writer);
        }
        transaction.commit();
        return segment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to update the segment " + segment.getName()) ;
    lock.execute(op, 3, 3000);
  }
  
  public List<String> getAllReaders() throws RegistryException {
    return readersAllNode.getChildren() ;
  }
  
  public List<String> getActiveReaders() throws RegistryException {
    return readersActiveNode.getChildren() ;
  }
  
  public List<String> getHistoryReaders() throws RegistryException {
    return readersHistoryNode.getChildren() ;
  }
  
  public NStorageReaderDescriptor getReader(String name) throws RegistryException {
    return readersAllNode.getChild(name).getDataAs(NStorageReaderDescriptor.class);
  }
  
  public NStorageReaderDescriptor createReader(String name) throws RegistryException {
    NStorageReaderDescriptor reader = new NStorageReaderDescriptor(readerIdTracker.nextInt(), name) ;
    Transaction transaction = registry.getTransaction();
    transaction.createChild(readersAllNode, reader.getId(), reader, NodeCreateMode.PERSISTENT);
    transaction.createChild(readersActiveNode, reader.getId(), NodeCreateMode.PERSISTENT);
    transaction.createChild(readersHeartbeatNode, reader.getId(), NodeCreateMode.EPHEMERAL);
    transaction.commit();
    return reader;
  }
  
  public List<String> getAllWriters() throws RegistryException {
    return writersAllNode.getChildren() ;
  }
  
  public List<String> getActiveWriters() throws RegistryException {
    return writersActiveNode.getChildren() ;
  }
  
  public List<String> getHistoryWriters() throws RegistryException {
    return writersHistoryNode.getChildren() ;
  }
  
  public NStorageWriterDescriptor getWriter(String id) throws RegistryException {
    return writersAllNode.getChild(id).getDataAs(NStorageWriterDescriptor.class);
  }
  
  public NStorageWriterDescriptor createWriter(String name) throws RegistryException {
    NStorageWriterDescriptor writer = new NStorageWriterDescriptor(writerIdTracker.nextInt(), name) ;
    Transaction transaction = registry.getTransaction();
    transaction.createChild(writersAllNode, writer.getId(), writer, NodeCreateMode.PERSISTENT);
    transaction.createChild(writersActiveNode, writer.getId(), NodeCreateMode.PERSISTENT);
    transaction.createChild(writersHeartbeatNode, writer.getId(), NodeCreateMode.EPHEMERAL);
    transaction.commit();
    return writer;
  }
  
  public void closeWriter(String id) throws RegistryException {
    closeWriter(getWriter(id));
  }
  
  public void closeWriter(NStorageWriterDescriptor writer) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    writer.setFinishedTime(System.currentTimeMillis());
    transaction.deleteChild(writersActiveNode,    writer.getId());
    transaction.deleteChild(writersHeartbeatNode, writer.getId());
    transaction.createChild(writersHistoryNode,   writer.getId(), NodeCreateMode.PERSISTENT);
    transaction.commit();
  }
}