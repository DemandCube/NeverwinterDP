package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

abstract public class NodeChildrenWatcher extends NodeWatcher {
  private Registry registry;
  private boolean persistent ;
  private String  watchedPath = null ;
  
  public NodeChildrenWatcher(Registry registry, boolean persistent) {
    this.registry   = registry;
    this.persistent = persistent;
  }
  
  public Registry getRegistry() { return this.registry; }
  
  public String getWatchedPath() { return this.watchedPath ; }
  
  public void watchChildren(String path) throws RegistryException {
    if(watchedPath != null) {
      throw new RegistryException(ErrorCode.Unknown, "Already watched " + watchedPath) ;
    }
    watchedPath = path ;
    registry.watchChildren(path, this);
  }
  
  @Override
  public void onEvent(NodeEvent event) {
    if(isComplete()) return;
    if(persistent) {
      try {
        registry.watchChildren(event.getPath(), this);
      } catch(RegistryException ex) {
        if(ex.getErrorCode() != ErrorCode.NoNode) {
          onNodeDelete();
        } else {
          onWatchError(event, ex);
        }
      }
    }
    
    try {
      processNodeEvent(event);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  abstract public void processNodeEvent(NodeEvent event) throws Exception ;
  
  protected void onNodeDelete() {
  }
  
  protected void onWatchError(NodeEvent event, RegistryException ex) {
    System.err.println("Stop watching " + event.getPath() + " due to the error: " + ex.getMessage());
  }
}