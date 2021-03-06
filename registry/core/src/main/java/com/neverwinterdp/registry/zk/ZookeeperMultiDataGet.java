package com.neverwinterdp.registry.zk;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.MultiDataGet;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;

public class ZookeeperMultiDataGet<T> implements MultiDataGet<T>, AsyncCallback.DataCallback {
  private RegistryImpl                      registry;
  private Class<T>                          type;
  private LinkedHashMap<String, DataGet<T>> results            = new LinkedHashMap<String, DataGet<T>>();
  private int                               processResultCount = 0;
  private int                               processErrorCount  = 0;
  private boolean                           shutdown           = false;

  public ZookeeperMultiDataGet(RegistryImpl registry, Class<T> type) {
    this.registry = registry;
    this.type = type;
  }

  @Override
  public int getProcessErrorGetCount() {
    return this.processErrorCount;
  }

  @Override
  public int getProcessResultCount() {
    return this.processResultCount;
  }

  @Override
  synchronized public void get(String path) throws RegistryException {
    if(shutdown) {
      throw new RuntimeException("MultiDataGet is already shutdown!");
    }
    ZooKeeper zk = registry.getZKClient();
    String realPath = registry.realPath(path);
    DataGet<T> dataGet = new DataGet<T>(path, null);
    results.put(path, dataGet);
    zk.getData(realPath, false, this, null);
  }

  @Override
  public void get(String... paths) throws RegistryException {
    for (String selPath : paths) {
      get(selPath);
    }
  }

  @Override
  public void get(List<String> paths) throws RegistryException {
    for (int i = 0; i < paths.size(); i++)
      get(paths.get(i));
  }

  @Override
  public void getChildren(String path) throws RegistryException {
    List<String> children = registry.getChildren(path);
    for (String child : children) {
      get(path + "/" + child);
    }
  }

  @Override
  public void getChildren(Node node) throws RegistryException {
    List<String> children = node.getChildren();
    for (String child : children) {
      get(node.getPath() + "/" + child);
    }
  }

  @Override
  public List<T> getResults() {
    List<T> holder = new ArrayList<T>();
    for (DataGet<T> sel : results.values()) {
      holder.add(sel.getData());
    }
    return holder;
  }

  @Override
  public List<DataGet<T>> getDataGetResults() {
    return null;
  }

  public void shutdown() {
    shutdown = true;
  }

  
  @Override
  synchronized public void waitForAllGet(long timeout) throws RegistryException {
    long stopTime = System.currentTimeMillis() + timeout;
    long waitTime = timeout;
    try {
      while (waitTime > 0) {
        System.out.println("wait time = " + waitTime);
        wait(waitTime);
        if(processResultCount == results.size() && shutdown) return;
        waitTime = stopTime - System.currentTimeMillis();
      }
    } catch (InterruptedException e) {
      throw new RegistryException(ErrorCode.Timeout, "Cannot retrieve the data in " + timeout + "ms");
    }
    System.out.println("end wait for all get, processResultCount = " + processResultCount + ", expect " + results.size());
  }

  @Override
  synchronized public void processResult(int rc, String realPath, Object ctx, byte[] data, Stat stat) {
    System.out.println("process result: " + realPath + ", rc = " + rc);
    processResultCount++;
    String path = registry.path(realPath);
    DataGet<T> dataGet = results.get(path);
    if (rc == KeeperException.Code.OK.intValue()) {
      dataGet.setData(JSONSerializer.INSTANCE.fromBytes(data, type));
    } else {
      processErrorCount++;
      dataGet.setErrorCode(ErrorCode.Unknown);
    }
    notifyAll();
  }
}