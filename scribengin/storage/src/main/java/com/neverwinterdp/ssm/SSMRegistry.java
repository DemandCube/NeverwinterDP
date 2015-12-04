package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;

public class SSMRegistry {
  final static public String READERS           = "readers";
  final static public String READERS_ACTIVE    = READERS + "/active";
  final static public String READERS_HISTORY   = READERS + "/history";
  final static public String READERS_HEARTBEAT = READERS + "/heartbeat";
  
  final static public String WRITERS           = "writers";
  final static public String WRITERS_ACTIVE    = WRITERS + "/active";
  final static public String WRITERS_HEARTBEAT = WRITERS + "/heartbeat";
  final static public String WRITERS_HISTORY   = WRITERS + "/history";
  
  private Registry registry;
  private String   registryPath;

  private Node registryNode;
  private Node segmentsNode;

  private Node              readersNode;
  private Node              readersActiveNode;
  private Node              readersHeartbeatNode;
  private Node              readersHistoryNode;

  private Node              writersNode;
  private Node              writersActiveNode;
  private Node              writersHeartbeatNode;
  private Node              writersHistoryNode;
  private SequenceIdTracker writerIdTracker;
  
  private Node actionQueueNode;

  private Node      lockNode;
  
  public SSMRegistry(Registry registry, String path) throws RegistryException {
    this.registry = registry;
    this.registryPath = path;
    
    registryNode       = registry.get(registryPath);
    segmentsNode       = registryNode.getChild("segments");
    
    readersNode          = registryNode.getChild(READERS);
    readersActiveNode    = registryNode.getDescendant(READERS_ACTIVE);
    readersHeartbeatNode = registryNode.getDescendant(READERS_HEARTBEAT);
    readersHistoryNode   = registryNode.getDescendant(READERS_HISTORY);
    
    writersNode          = registryNode.getChild(WRITERS);
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
    trans.create(readersActiveNode,       null, NodeCreateMode.PERSISTENT);
    trans.create(readersHeartbeatNode, null, NodeCreateMode.PERSISTENT);
    trans.create(readersHistoryNode,   null, NodeCreateMode.PERSISTENT);
    
    trans.create(writersNode,          null, NodeCreateMode.PERSISTENT);
    trans.create(writersActiveNode,       null, NodeCreateMode.PERSISTENT);
    trans.create(writersHeartbeatNode, null, NodeCreateMode.PERSISTENT);
    trans.create(writersHistoryNode,   null, NodeCreateMode.PERSISTENT);
    writerIdTracker.initRegistry(trans);
    
    trans.create(actionQueueNode,    null, NodeCreateMode.PERSISTENT);
    trans.create(lockNode,           null, NodeCreateMode.PERSISTENT);
  }
  
  public boolean exists() throws RegistryException { 
    return registryNode.exists(); 
  }
  
  public Registry getRegistry() { return registry ; }
  
  public String getRegistryPath() { return registryPath ; }
  
  public List<String> getSegments() throws RegistryException {
    List<String> segments = segmentsNode.getChildren() ;
    Collections.sort(segments);
    return segments;
  }
  
  public SegmentDescriptor getSegmentById(int id) throws RegistryException {
    return segmentsNode.getChild(SegmentDescriptor.toSegmentId(id)).getDataAs(SegmentDescriptor.class);
  }
  
  public SegmentDescriptor getSegmentBySegmentId(String name) throws RegistryException {
    return segmentsNode.getChild(name).getDataAs(SegmentDescriptor.class);
  }
  
  public SegmentDescriptor getNextSegmentDescriptor(int segmentId) throws RegistryException {
    Node nextSegmentNode = segmentsNode.getChild(SegmentDescriptor.toSegmentId(segmentId + 1));
    try {
      SegmentDescriptor nextSegment = nextSegmentNode.getDataAs(SegmentDescriptor.class);
      return nextSegment;
    } catch(RegistryException ex) {
      if(ex.getErrorCode() == ErrorCode.NoNode) return null;
      throw ex;
    }
  }

  public SegmentDescriptor newSegment(final SSMWriterDescriptor writer) throws RegistryException {
    BatchOperations<SegmentDescriptor> op = new BatchOperations<SegmentDescriptor>() {
      @Override
      public SegmentDescriptor execute(Registry registry) throws RegistryException {
        List<String> segments = segmentsNode.getChildren();
        SegmentDescriptor segment = new SegmentDescriptor(segments.size());
        segment.setCreator(writer.getWriter());
        Transaction transaction = registry.getTransaction();
        transaction.createChild(segmentsNode, segment.getSegmentId(), segment, NodeCreateMode.PERSISTENT);
//        transaction.createDescendant(segmentsNode, segment.getSegmentId() + "/lock", NodeCreateMode.PERSISTENT) ;
//        transaction.createDescendant(segmentsNode, segment.getSegmentId() + "/data", NodeCreateMode.PERSISTENT) ;
        
        writer.logStartSegment(segment.getSegmentId());
        transaction.setData(writersActiveNode.getPath() + "/" + writer.getId(), writer);
        transaction.commit();
        return segment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to create a new segment") ;
    return lock.execute(op, 3, 3000);
  }
  
  public void commit(SSMWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException {
    save(writer, segment, false);
  }
  
  public void finish(SSMWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException {
    save(writer, segment, true);
  }
  
  void save(final SSMWriterDescriptor writer, final SegmentDescriptor segment, final boolean finished) throws RegistryException {
    BatchOperations<SegmentDescriptor> op = new BatchOperations<SegmentDescriptor>() {
      @Override
      public SegmentDescriptor execute(Registry registry) throws RegistryException {
        Transaction transaction = registry.getTransaction();
        Node segNode = segmentsNode.getChild(segment.getSegmentId());
        transaction.setData(segNode.getPath(), segment);
        if(finished) {
          writer.logFinishSegment();
          transaction.setData(writersActiveNode.getPath() + "/" + writer.getId(), writer);
        }
        transaction.commit();
        return segment;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to update the segment " + segment.getSegmentId()) ;
    lock.execute(op, 3, 3000);
  }
  
  public List<String> cleanReadSegmentByActiveReader() throws RegistryException {
    BatchOperations<List<String>> op = new BatchOperations<List<String>>() {
      @Override
      public List<String> execute(Registry registry) throws RegistryException {
        System.err.println("cleanReadSegmentByActiveReader(): " + registryPath);
        List<String> deleteSegments = new ArrayList<>();
        List<String> readers = readersActiveNode.getChildren() ;
        String minReadSegmentId = null;
        for(int i = 0; i < readers.size(); i++) {
          String reader = readers.get(i);
          System.err.println("  reader: " + readers);
          List<String> readSegments = readersActiveNode.getChild(reader).getChildren();
          System.err.println("    read segments: " + readSegments);
          String readerMinReadSegment = null;
          if(readSegments.size() == 0)  {
            SSMReaderDescriptor readerDescriptor = readersActiveNode.getChild(reader).getDataAs(SSMReaderDescriptor.class);
            readerMinReadSegment = readerDescriptor.getLastReadSegmentId();
          } else {
            Collections.sort(readSegments);
            readerMinReadSegment = readSegments.get(0);
          }
          if(minReadSegmentId == null) minReadSegmentId = readerMinReadSegment;
          else if(minReadSegmentId.compareTo(readerMinReadSegment) > 0) minReadSegmentId = readerMinReadSegment;
        }
        System.err.println("  minReadSegmentId: " + minReadSegmentId);
        if(minReadSegmentId == null) return deleteSegments;
        
        List<String> segments = segmentsNode.getChildren();
        Transaction transaction = registry.getTransaction();
        for(int i = 0; i < segments.size(); i++) {
          String segmentId = segments.get(i);
          if(segmentId.compareTo(minReadSegmentId) < 0) {
            transaction.deleteChild(segmentsNode, segmentId);
            deleteSegments.add(segmentId);
          }
        }
        transaction.commit();
        System.err.println("  delete: " + deleteSegments);
        return deleteSegments;
      }
    };
    Lock lock = lockNode.getLock("write", "Lock to remove the segments that are already read by the active reader") ;
    return lock.execute(op, 3, 3000);
  }
  
  public List<String> getActiveReaders() throws RegistryException {
    return readersActiveNode.getChildren() ;
  }
  
  
  public List<String> getHistoryReaders() throws RegistryException {
    return readersHistoryNode.getChildren() ;
  }
  
  public SSMReaderDescriptor getReader(String name) throws RegistryException {
    return readersActiveNode.getChild(name).getDataAs(SSMReaderDescriptor.class);
  }
  
  public SSMReaderDescriptor getOrCreateReader(String readerId) throws RegistryException {
    SSMReaderDescriptor reader = new SSMReaderDescriptor(readerId) ;
    Transaction transaction = registry.getTransaction();
    transaction.createChild(readersActiveNode, reader.getReaderId(), reader, NodeCreateMode.PERSISTENT);
    transaction.createChild(readersHeartbeatNode, reader.getReaderId(), NodeCreateMode.EPHEMERAL);
    transaction.commit();
    return reader;
  }
  
  public SegmentReadDescriptor createSegmentReadDescriptor(SSMReaderDescriptor reader, SegmentDescriptor segment) throws RegistryException {
    Node readerNode = readersActiveNode.getChild(reader.getReaderId());
    Node readSegmentNode = readerNode.getChild(segment.getSegmentId());
    Transaction trans = registry.getTransaction();
    SegmentReadDescriptor segReadDescriptor = new SegmentReadDescriptor(segment.getSegmentId());
    reader.setLastReadSegmentId(segment.getSegmentId());
    trans.create(readSegmentNode, segReadDescriptor, NodeCreateMode.PERSISTENT);
    trans.setData(readerNode, reader);
    trans.commit();
    return segReadDescriptor;
  }
  
  public SegmentReadDescriptor createNextSegmentReadDescriptor(SSMReaderDescriptor reader, SegmentDescriptor segment) throws RegistryException {
    Node nextSegmentNode = segmentsNode.getChild(SegmentDescriptor.toSegmentId(segment.getId() + 1));
    if(nextSegmentNode.exists()) {
      SegmentDescriptor nextSegment = nextSegmentNode.getDataAs(SegmentDescriptor.class);
      return createSegmentReadDescriptor(reader, nextSegment);
    }
    return null;
  }
  
  public void commit(Transaction trans, SSMReaderDescriptor reader, SegmentDescriptor segment, SegmentReadDescriptor segRead, boolean complete) throws RegistryException {
    Node readerNode      = readersActiveNode.getChild(reader.getReaderId());
    Node readSegmentNode = readerNode.getChild(segment.getSegmentId());
    if(complete || segment.getStatus() == SegmentDescriptor.Status.COMPLETE) {
      if(complete || segment.getDataSegmentLastCommitPos() == segRead.getCommitReadDataPosition()) {
        trans.delete(readSegmentNode.getPath());
      } else {
        trans.setData(readSegmentNode, segRead);
      }
    } else {
      trans.setData(readSegmentNode, segRead);
    }
  }
  
  public List<String> getSegmentReadDescriptors(SSMReaderDescriptor reader) throws RegistryException {
    Node readerNode = readersActiveNode.getChild(reader.getReaderId());
    List<String> readSegments = readerNode.getChildren();
    Collections.sort(readSegments);
    return readSegments;
  }
  
  public SegmentReadDescriptor getSegmentReadDescriptor(SSMReaderDescriptor reader, String segmentId) throws RegistryException {
    Node readerNode = readersActiveNode.getChild(reader.getReaderId());
    Node segmentReadNode = readerNode.getChild(segmentId);
    return segmentReadNode.getDataAs(SegmentReadDescriptor.class);
  }
  
  public void removeReader(String id) throws RegistryException {
    removeReader(getReader(id));
  }
  
  public void removeReader(SSMReaderDescriptor reader) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    reader.setFinishedTime(System.currentTimeMillis());
    transaction.deleteChild(readersHeartbeatNode, reader.getReaderId());
    transaction.rdelete(readersActiveNode.getPath() + "/" + reader.getReaderId());
    transaction.createChild(readersHistoryNode, reader.getReaderId() + "-", reader, NodeCreateMode.PERSISTENT_SEQUENTIAL);
    transaction.commit();
  }
  
  public List<String> getActiveWriters() throws RegistryException {
    return writersActiveNode.getChildren() ;
  }
  
  public List<String> getHistoryWriters() throws RegistryException {
    return writersHistoryNode.getChildren() ;
  }
  
  public SSMWriterDescriptor getWriter(String id) throws RegistryException {
    return writersActiveNode.getChild(id).getDataAs(SSMWriterDescriptor.class);
  }
  
  public SSMWriterDescriptor createWriter(String name) throws RegistryException {
    SSMWriterDescriptor writer = new SSMWriterDescriptor(writerIdTracker.nextInt(), name) ;
    Transaction transaction = registry.getTransaction();
    transaction.createChild(writersActiveNode, writer.getId(), writer, NodeCreateMode.PERSISTENT);
    transaction.createChild(writersHeartbeatNode, writer.getId(), NodeCreateMode.EPHEMERAL);
    transaction.commit();
    return writer;
  }
  
  public void removeWriter(String id) throws RegistryException {
    removeWriter(getWriter(id));
  }
  
  public void removeWriter(SSMWriterDescriptor writer) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    writer.setFinishedTime(System.currentTimeMillis());
    transaction.deleteChild(writersHeartbeatNode, writer.getId());
    transaction.deleteChild(writersActiveNode, writer.getId());
    transaction.createChild(writersHistoryNode,   writer.getId() + "-", writer, NodeCreateMode.PERSISTENT_SEQUENTIAL);
    transaction.commit();
  }
}