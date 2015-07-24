package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ClusterMetterSnapshot implements Serializable {
  private String name ;
  private MetterSnapshot metter ;
  private Map<String, MetterSnapshot> metters = new HashMap<String, MetterSnapshot>() ;

  public ClusterMetterSnapshot() { }

  public ClusterMetterSnapshot(String name) {
    this.name = name;
  }

  public String getName() { return name; }
  public void setName(String name) {
    this.name = name;
  }

  public MetterSnapshot getMetter() { return metter; }
  public void setMetter(MetterSnapshot metter) { this.metter = metter; }

  public Map<String, MetterSnapshot> getMetters() { return metters; }
  public void setMetters(Map<String, MetterSnapshot> metters) {
    this.metters = metters;
  }
  
  public void add(MetterSnapshot snapshot) {
    if(!name.equals(snapshot.getName())) {
      throw new RuntimeException("expect name " + name) ;
    }
    metters.put(snapshot.getServerName(), snapshot);
  }
}
