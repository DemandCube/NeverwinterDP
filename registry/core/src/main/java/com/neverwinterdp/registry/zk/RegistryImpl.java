package com.neverwinterdp.registry.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import com.google.inject.Inject;
import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.DataMapperCallback;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.MultiDataGet;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeInfo;
import com.neverwinterdp.registry.PathFilter;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.LoggerFactory;

public class RegistryImpl implements Registry {
  static public final Id ANYONE_ID = new Id("world", "anyone");
  static public final ArrayList<ACL> DEFAULT_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID)));
  
  static AtomicInteger idTracker = new AtomicInteger();
  
  private int clientId = idTracker.incrementAndGet();
  
  private Logger logger ;
  
  private RegistryConfig config;
  
  private ZooKeeper zkClient ;
  
  private boolean closed = false;
  private int     reconnectCount = 0;
  
  public RegistryImpl(RegistryConfig config) {
    this.config = config;
  }
  
  public RegistryConfig getRegistryConfig() { return config; }
  
  public String getSessionId() {
    if(zkClient == null) return null ;
    return Long.toString(zkClient.getSessionId()) ;
  }
  
  @Inject
  public void init(LoggerFactory lFactory) throws RegistryException {
    logger = lFactory.getLogger(RegistryImpl.class);
  }
  
  @Override
  public Registry connect() throws RegistryException {
    closed = false;
    return reconnect(15000) ;
  }
  
  @Override
  synchronized public Registry reconnect(long timeout) throws RegistryException {
    if(closed) {
      throw new RegistryException(ErrorCode.Closed, "Registry has been closed");
    }
    info("Reconnect to the registry, connect count = " + ++reconnectCount);
    
    try {
      if(zkClient != null) zkClient.close();
      ZkConnectedStateWatcher connectStateWatcher = new ZkConnectedStateWatcher();
      zkClient = new ZooKeeper(config.getConnect(), 10000, connectStateWatcher);
      if(!connectStateWatcher.waitForConnected(10000)) {
        zkClient.close();
        zkClient = null;
        throw new RegistryException(ErrorCode.Connection, "Cannot connect due to the interrupt") ;
      }
    } catch(IOException | InterruptedException ex) {
      throw new RegistryException(ErrorCode.Connection, ex) ;
    }
    zkCreateIfNotExist(config.getDbDomain()) ;
    return this;
  }

  @Override
  synchronized public void shutdown() throws RegistryException {
    if(closed) return;
    closed = true;
    if(zkClient != null) {
      try {
        zkClient.close();;
        zkClient = null ;
      } catch (InterruptedException e) {
        throw new RegistryException(ErrorCode.Connection, e) ;
      }
    }
  }
  
  @PreDestroy
  public void onDestroy() throws RegistryException {
    shutdown();
  }

  ZooKeeper getZKClient() throws RegistryException {
    if(closed) {
      throw new RegistryException(ErrorCode.Closed, "Registry has been closed");
    }
    return zkClient;
  }
  
  @Override
  public boolean isConnect() { 
    return zkClient != null  && zkClient.getState().isConnected() ;
  }
  
  
  @Override
  public Node create(String path, NodeCreateMode mode) throws RegistryException {
    return create(path, new byte[0], mode);
  }
  
  @Override
  public void createRef(String path, String toPath, NodeCreateMode mode) throws RegistryException {
    RefNode refNode = new RefNode();
    refNode.setPath(toPath);
    create(path, refNode, mode);
  }
  
  @Override
  public  Node create(final String path, final byte[] data, final NodeCreateMode mode) throws RegistryException {
    Operation<Node> create = new Operation<Node>() {
      @Override
      public Node execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        String creationPath = path;
        String retPath = zkClient.create(realPath(path), data, DEFAULT_ACL, toCreateMode(mode)) ;
        if(mode == NodeCreateMode.PERSISTENT_SEQUENTIAL || mode == NodeCreateMode.EPHEMERAL_SEQUENTIAL) {
          creationPath = retPath.substring(config.getDbDomain().length()) ;
        }
        return new Node(RegistryImpl.this, creationPath);
      }
    };
    return execute(create, 3);
  }
  
  @Override
  public <T> Node create(String path, T data, NodeCreateMode mode) throws RegistryException {
    return create(path, JSONSerializer.INSTANCE.toBytes(data), mode);
  }
  
  @Override
  public Node createIfNotExist(String path) throws RegistryException {
    zkCreateIfNotExist(realPath(path));
    return new Node(this, path) ;
  }
  
  @Override
  public Node get(String path) { return new Node(this, path) ; }
  
  @Override
  public NodeInfo getInfo(final String path) throws RegistryException {
    Operation<NodeInfo> getInfo = new Operation<NodeInfo>() {
      @Override
      public NodeInfo execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists(realPath(path), false);
        return new ZKNodeInfo(stat);
      }
    };
    return execute(getInfo, 3);
  }
  
  @Override
  public Node getRef(String path) throws RegistryException {
    RefNode refNode = retrieveDataAs(path, RefNode.class);
    return new Node(this, refNode.getPath()) ;
  }
  
  @Override
  public <T> T getRefAs(String path, Class<T> type) throws RegistryException {
    RefNode refNode = retrieveDataAs(path, RefNode.class);
    return retrieveDataAs(refNode.getPath(), type);
  }
  
  @Override
  public <T> List<T> getRefAs(List<String> path, Class<T> type) throws RegistryException {
    List<T> holder = new ArrayList<T>();
    for(String selPath : path) {
      RefNode refNode = retrieveDataAs(selPath, RefNode.class);
      holder.add(retrieveDataAs(refNode.getPath(), type)) ;
    }
    return holder ;
  }
  
  @Override
  public byte[] getData(final String path) throws RegistryException {
    Operation<byte[]> getData = new Operation<byte[]>() {
      @Override
      public byte[] execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        return zkClient.getData(realPath(path), null, new Stat()) ;
      }
    };
    return execute(getData, 3);
  }
  
  @Override
  public <T> T getDataAs(String path, Class<T> type) throws RegistryException {
    return retrieveDataAs(path, type);
  }
  
  <T> T retrieveDataAs(final String path, final Class<T> type) throws RegistryException {
    Operation<T> retrieveDataAs = new Operation<T>() {
      @Override
      public T execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        byte[] bytes =  zkClient.getData(realPath(path), null, new Stat()) ;
        if(bytes == null || bytes.length == 0) return null;
        return JSONSerializer.INSTANCE.fromBytes(bytes, type);
      }
    };
    return execute(retrieveDataAs, 3);
  }
  
  public <T> T getDataAs(final String path, final Class<T> type, final DataMapperCallback<T> mapper) throws RegistryException {
    Operation<T> getDataAs = new Operation<T>() {
      @Override
      public T execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        byte[] bytes =  zkClient.getData(realPath(path), null, new Stat()) ;
        if(bytes == null || bytes.length == 0) return null;
        return mapper.map(path, bytes, type);
      }
    };
    return execute(getDataAs, 3);
  }
  
  @Override
  public <T> List<T> getDataAs(List<String> paths, Class<T> type) throws RegistryException {
    return getDataAs(paths, type, false);
  }
  
  @Override
  public <T> List<T> getDataAs(final List<String> paths, final Class<T> type, final boolean ignoreNoNodeError) throws RegistryException {
    Operation<List<T>> getDataAs = new Operation<List<T>>() {
      @Override
      public List<T> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException {
        List<T> holder = new ArrayList<T>();
        for(String path : paths) {
          T obj = retrieveDataAs(path, type);
          holder.add(obj);
        }
        return holder;
      }
    };
    return execute(getDataAs, 3);
  }
  
  
  @Override
  public <T> List<T> getDataAs(List<String> paths, Class<T> type, DataMapperCallback<T> mapper) throws RegistryException {
    List<T> holder = new ArrayList<T>();
    for(String path : paths) {
      holder.add(getDataAs(path, type, mapper));
    }
    return holder;
  }
  
  public <T> MultiDataGet<T> createMultiDataGet(Class<T> type) {
    return new ZookeeperMultiDataGet<T>(this, type);
  }
  
  public NodeInfo setData(final String path, final byte[] data) throws RegistryException {
    Operation<NodeInfo> setData = new Operation<NodeInfo>() {
      @Override
      public NodeInfo execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        Stat stat = zkClient.setData(realPath(path), data, -1) ;
        return new ZKNodeInfo(stat);
      }
    };
    return execute(setData, 3);
  }
  
  public <T> NodeInfo setData(String path, T data) throws RegistryException {
    return setData(path, JSONSerializer.INSTANCE.toBytes(data));
  }
  
  
  public List<String> getChildren(final String path) throws RegistryException {
    Operation<List<String>> getChildren = new Operation<List<String>>() {
      @Override
      public List<String> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        List<String> names = zkClient.getChildren(realPath(path), false);
        return names ;
      }
    };
    return execute(getChildren, 3);
  }
  
  public List<String> getChildrenPath(final String path) throws RegistryException {
    Operation<List<String>> getChildrenPath = new Operation<List<String>>() {
      @Override
      public List<String> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        List<String> names = zkClient.getChildren(realPath(path), false);
        List<String> paths = new ArrayList<String>() ;
        for(String name : names) {
          paths.add(path + "/" + name);
        }
        return names ;
      }
    };
    return execute(getChildrenPath, 3);
  }
  
  public List<String> getChildren(final String path, final boolean watch) throws RegistryException {
    Operation<List<String>> getChildren = new Operation<List<String>>() {
      @Override
      public List<String> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException {
        List<String> names = zkClient.getChildren(realPath(path), watch);
        return names ;
      }
    };
    return execute(getChildren, 3);
  }
  
  public <T> List<T> getChildrenAs(String path, Class<T> type) throws RegistryException {
    return getChildrenAs(path, type, false);
  }
  
  public <T> List<T> getChildrenAs(final String path, final Class<T> type, final boolean ignoreNoNodeError) throws RegistryException {
    Operation<List<T>> getChildrenAs = new Operation<List<T>>() {
      @Override
      public List<T> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException {
        List<T> holder = new ArrayList<T>();
        List<String> nodes = zkClient.getChildren(realPath(path), false);
        Collections.sort(nodes);
        for(int i = 0; i < nodes.size(); i++) {
          String name = nodes.get(i) ;
          try {
            T object = retrieveDataAs(path + "/" + name, type);
            holder.add(object);
          } catch(RegistryException ex) {
            if(ex.getErrorCode() == ErrorCode.NoNode && ignoreNoNodeError) continue;
          }
        }
        return holder ;
      }
    };
    return execute(getChildrenAs, 3);
  }
  
  @Override
  public <T> List<T> getChildrenAs(String path, Class<T> type, DataMapperCallback<T> callback) throws RegistryException {
    return getChildrenAs(path, type, callback, false);
  }
  
  @Override
  public <T> List<T> getChildrenAs(final String path, final Class<T> type, final DataMapperCallback<T> callback, final boolean ignoreNoNodeError) throws RegistryException {
    Operation<List<T>> getChildrenAs = new Operation<List<T>>() {
      @Override
      public List<T> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException {
        List<T> holder = new ArrayList<T>();
        List<String> nodes = getChildren(path);
        Collections.sort(nodes);
        for(int i = 0; i < nodes.size(); i++) {
          String name = nodes.get(i) ;
          try {
            T object = getDataAs(path + "/" + name, type, callback);
            holder.add(object);
          } catch(RegistryException ex) {
            if(ex.getErrorCode() == ErrorCode.NoNode && ignoreNoNodeError) continue;
            throw ex;
          }
        }
        return holder ;
      }
    };
    return execute(getChildrenAs, 3);
  }
  
  @Override
  public <T> List<T> getRefChildrenAs(String path, Class<T> type) throws RegistryException {
    List<RefNode> refNodes = getChildrenAs(path, RefNode.class) ;
    List<String> paths = new ArrayList<>() ;
    for(int i = 0; i < refNodes.size(); i++) {
      paths.add(refNodes.get(i).getPath());
    }
    return getDataAs(paths, type);
  }

  @Override
  public <T> List<T> getRefChildrenAs(String path, Class<T> type, boolean ignoreNoNodeError) throws RegistryException {
    List<RefNode> refNodes = getChildrenAs(path, RefNode.class) ;
    List<String> paths = new ArrayList<>() ;
    for(int i = 0; i < refNodes.size(); i++) {
      paths.add(refNodes.get(i).getPath());
    }
    return getDataAs(paths, type, ignoreNoNodeError);
  }

  
  @Override
  public boolean exists(final String path) throws RegistryException {
    Operation<Boolean> exists = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists(realPath(path), false) ;
        if(stat != null) return true ;
        return false ;
      }
    };
    return execute(exists, 3);
  }
  
  @Override
  public boolean watchModify(final String path, final NodeWatcher watcher) throws RegistryException {
    Operation<Boolean> watchModify = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        zkClient.getData(realPath(path), new ZKNodeWatcher(config.getDbDomain(), watcher), new Stat()) ;
        return true;
      }
    };
    return execute(watchModify, 3);
  }
  
  @Override
  public void watchExists(final String path, final NodeWatcher watcher) throws RegistryException {
    Operation<Boolean> watchExists = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        zkClient.exists(realPath(path), new ZKNodeWatcher(config.getDbDomain(), watcher)) ;
        return true;
      }
    };
    execute(watchExists, 3);
  }
  
  @Override
  public void watchChildren(final String path, final NodeWatcher watcher) throws RegistryException {
    Operation<Boolean> watchChildren = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        zkClient.getChildren(realPath(path), new ZKNodeWatcher(config.getDbDomain(), watcher)) ;
        return true;
      }
    };
    execute(watchChildren, 3);
  }
  
  @Override
  public void delete(final String path) throws RegistryException {
    Operation<Boolean> delete = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        zkClient.delete(realPath(path), -1);
        return true;
      }
    };
    execute(delete, 3);
  }
 
  @Override
  public void rdelete(final String path) throws RegistryException {
    Operation<Boolean> rdelete = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException {
        Transaction transaction = getTransaction();
        transaction.rdelete(path);
        transaction.commit();
        return true;
      }
    };
    execute(rdelete, 3);
  }
  
  public List<String> findDencendantPaths(String path) throws RegistryException {
    return findDencendantPaths(path, null);
  }
  
  public List<String> findDencendantPaths(String path, PathFilter filter) throws RegistryException {
    List<String> paths = findDencendantRealPaths(path, filter) ;
    List<String> holder = new ArrayList<String>() ;
    for(int i = 0; i < paths.size(); i++) {
      String selPath = paths.get(i) ;
      selPath = selPath.substring(config.getDbDomain().length());
      holder.add(selPath);
    }
    return holder ;
  }
  
  /**
   * BFS Traversal of the system under pathRoot, with the entries in the list, in the 
   * same order as that of the traversal.
   * <p>
   * <b>Important:</b> This is <i>not an atomic snapshot</i> of the tree ever, but the
   *  state as it exists across multiple RPCs from zkClient to the ensemble.
   * For practical purposes, it is suggested to bring the clients to the ensemble 
   * down (i.e. prevent writes to pathRoot) to 'simulate' a snapshot behavior.   
   * 
   * @param zk the zookeeper handle
   * @param pathRoot The znode path, for which the entire subtree needs to be listed.
   * @throws InterruptedException 
   * @throws KeeperException 
   */
  List<String> findDencendantRealPaths(final String path, final PathFilter filter) throws RegistryException {
    Operation<List<String>> find = new Operation<List<String>>() {
      @Override
      public List<String> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        String pathRoot =  realPath(path);
        Deque<String> queue = new LinkedList<String>();
        List<String> tree = new ArrayList<String>();
        queue.add(pathRoot);
        boolean accept = true;
        if(filter != null) {
          accept = filter.accept(pathRoot);
        }
        if(accept) tree.add(pathRoot);
        while (true) {
          String node = queue.pollFirst();
          if (node == null) {
            break;
          }
          List<String> children = zkClient.getChildren(node, false);
          for (final String child : children) {
            final String childPath = node + "/" + child;
            accept = true;
            if(filter != null) {
              accept = filter.accept(childPath);
            }
            if(accept) {
              queue.add(childPath);
              tree.add(childPath);
            }
          }
        }
        return tree;
      }
    };
    return execute(find, 3);
  }
  
  public void rcopy(String path, String toPath) throws RegistryException {
    rcopy(path, toPath, null);
  }
  
  public void rcopy(final String path, final String toPath, final PathFilter filter) throws RegistryException {
    Operation<Boolean> rcopy = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException {
        Transaction transaction = getTransaction();
        transaction.rcopy(path, toPath, filter);
        transaction.commit();
        return true;
      }
    };
    execute(rcopy, 3);
  }
  
  @Override
  public Transaction getTransaction() throws RegistryException {
    ZooKeeper zkClient = getZKClient();
    return new TransactionImpl(this, zkClient.transaction());
  }
  
  public <T> T executeBatch(BatchOperations<T> ops, int retry, long timeoutThreshold) throws RegistryException {
    T result = ops.execute(this);
    return result;
  }
  
  public <T> T executeBatchWithRetry(BatchOperations<T> ops, int retry, long timeoutThreshold) throws RegistryException {
    RegistryException error = null;
    for(int i = 0;i < retry; i++) {
      try {
        T result = ops.execute(this);
        return result;
      } catch (RegistryException e) {
        if(e.getErrorCode().isConnectionProblem()) {
          reconnect(10000);
          error = e;
        } else {
          throw e;
        }
      }
    }
    throw error;
  }
  
  @Override
  public Registry newRegistry() throws RegistryException {
    return new RegistryImpl(config);
  }
  
  void zkCreateIfNotExist(final String path) throws RegistryException {
    Operation<Boolean> createIfNotExists = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        if (zkClient.exists(path, false) != null) return true;
        StringBuilder pathB = new StringBuilder();
        String[] pathParts = path.split("/");
        for(String pathEle : pathParts) {
          if(pathEle.length() == 0) continue ; //root
          pathB.append("/").append(pathEle);
          String pathString = pathB.toString();
          //bother with the exists call or not?
          Stat nodeStat = zkClient.exists(pathString, false);
          if (nodeStat == null) {
            try {
              zkClient.create(pathString, null, DEFAULT_ACL, CreateMode.PERSISTENT);
            } catch(KeeperException.NodeExistsException ex) {
              break;
            }
          }
        }
        return true;
      }
    };
    execute(createIfNotExists, 3);
  }
  
  String realPath(String path) { 
    if(path.equals("/")) return config.getDbDomain() ;
    return config.getDbDomain() + path; 
  }
  
  String path(String realPath) { 
    return realPath.substring(config.getDbDomain().length());
  }
  
  <T> T execute(Operation<T> op, int retry) throws RegistryException {
    if(closed) {
      throw new RegistryException(ErrorCode.Closed, "Registry has been closed");
    }
    RegistryException error = null;
    try {
      ZooKeeper zkClient = getZKClient();
      T result = op.execute(zkClient);
      return result;
    } catch(InterruptedException ex) {
      error = new RegistryException(ErrorCode.Timeout, "Interrupt Exception", ex) ;
    } catch(KeeperException kEx) {
      if(kEx.code() ==  KeeperException.Code.NONODE) {
        KeeperException.NoNodeException noNodeEx = (KeeperException.NoNodeException) kEx;
        String message = kEx.getMessage() + "\n" + "  path = " + noNodeEx.getPath();
        error = new RegistryException(ErrorCode.NoNode, message, kEx) ;
      } else if(kEx.code() ==  KeeperException.Code.NODEEXISTS) {
        KeeperException.NodeExistsException nodeExistsEx = (KeeperException.NodeExistsException) kEx;
        String message = kEx.getMessage() + "\n" + "  path = " + nodeExistsEx.getPath();
        error = new RegistryException(ErrorCode.NodeExists, message, kEx) ;
      } else if(kEx.code() ==  KeeperException.Code.CONNECTIONLOSS) {
        error = new RegistryException(ErrorCode.ConnectionLoss, kEx.getMessage(), kEx) ;
      } else {
        error = new RegistryException(ErrorCode.Unknown, "Unknown Error", kEx) ;
      }
    }
    if(error.getErrorCode().isConnectionProblem()) {
      error("Call shutdown", error);
      shutdown();
    }
    throw error;
  }
  
  <T> T executeWithRetry(Operation<T> op, int retry) throws RegistryException {
    RegistryException error = null ;
    for(int i = 0; i < retry; i++) {
      try {
        ZooKeeper zkClient = getZKClient();
        T result = op.execute(zkClient);
        return result;
      } catch(InterruptedException ex) {
        throw new RegistryException(ErrorCode.Timeout, "Interrupt Exception", ex) ;
      } catch(KeeperException kEx) {
        if(kEx.code() ==  KeeperException.Code.NONODE) {
          KeeperException.NoNodeException noNodeEx = (KeeperException.NoNodeException) kEx;
          String message = kEx.getMessage() + "\n" + "  path = " + noNodeEx.getPath();
          throw new RegistryException(ErrorCode.NoNode, message, kEx) ;
        } else if(kEx.code() ==  KeeperException.Code.NODEEXISTS) {
          KeeperException.NodeExistsException nodeExistsEx = (KeeperException.NodeExistsException) kEx;
          String message = kEx.getMessage() + "\n" + "  path = " + nodeExistsEx.getPath();
          throw new RegistryException(ErrorCode.NodeExists, message, kEx) ;
        } else if(kEx.code() ==  KeeperException.Code.CONNECTIONLOSS) {
          reconnect(10000);
          error = new RegistryException(ErrorCode.ConnectionLoss, kEx.getMessage(), kEx) ;
        }
      } catch(RegistryException ex) {
        throw ex;
      }
    }
    throw error;
  }
  
  void info(String message) {
    if(logger != null) logger.info("[clientId = " + clientId + "] " + message);
    else System.out.println("RegistryImpl[clientId = " + clientId + "] " + message);
  }
  
  void error(String message, Throwable t) {
    if(logger != null) {
      logger.error("[clientId = " + clientId + "] " + message, t);
    } else {
      System.out.println("RegistryImpl[clientId = " + clientId + "] " + message);
      t.printStackTrace();
    }
  }
  
  
  
  static public interface Operation<T> {
    public T execute(ZooKeeper zkClient) throws InterruptedException, KeeperException, RegistryException ;
  }

  static CreateMode toCreateMode(NodeCreateMode mode) {
    if(mode == NodeCreateMode.PERSISTENT) return CreateMode.PERSISTENT ;
    else if(mode == NodeCreateMode.PERSISTENT_SEQUENTIAL) return CreateMode.PERSISTENT_SEQUENTIAL ;
    else if(mode == NodeCreateMode.EPHEMERAL) return CreateMode.EPHEMERAL ;
    else if(mode == NodeCreateMode.EPHEMERAL_SEQUENTIAL) return CreateMode.EPHEMERAL_SEQUENTIAL ;
    throw new RuntimeException("Mode " + mode + " is not supported") ;
  }
  
  static public RegistryException toRegistryException(String message, Throwable t) {
    if(t instanceof InterruptedException) {
      return new RegistryException(ErrorCode.Timeout, message, t) ;
    } else if(t instanceof KeeperException) {
      KeeperException kEx = (KeeperException) t;
      if(kEx.code() ==  KeeperException.Code.NONODE) {
        KeeperException.NoNodeException noNodeEx = (KeeperException.NoNodeException) kEx;
        message += "\n" + "  path = " + noNodeEx.getPath();
        return new RegistryException(ErrorCode.NoNode, message, t) ;
      } else if(kEx.code() ==  KeeperException.Code.NODEEXISTS) {
        KeeperException.NodeExistsException nodeExistsEx = (KeeperException.NodeExistsException) kEx;
        message += "\n" + "  path = " + nodeExistsEx.getPath();
        return new RegistryException(ErrorCode.NodeExists, message, t) ;
      } else if(kEx.code() ==  KeeperException.Code.CONNECTIONLOSS) {
        return new RegistryException(ErrorCode.ConnectionLoss, message, t) ;
      }
    }
    return new RegistryException(ErrorCode.Unknown, message, t) ;
  }
}