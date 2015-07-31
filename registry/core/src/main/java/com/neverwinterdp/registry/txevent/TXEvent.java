package com.neverwinterdp.registry.txevent;

import java.util.UUID;

import com.neverwinterdp.util.JSONSerializer;

public class TXEvent {
  private String    id ;
  private String    name ;
  private long      expiredTime ;
  private byte[]    data ;

  public TXEvent() {
  }

  
  public TXEvent(String name, long expiredTime, byte[] data) {
    this.id   = name + "-" + UUID.randomUUID().toString();
    this.name = name;
    this.expiredTime = expiredTime ;
    this.data = data ;
  }

  public <T> TXEvent(String name, long expiredTime, T obj) {
    this(name, expiredTime,JSONSerializer.INSTANCE.toBytes(obj));
  }

  public String getId() { return id; }
  public void setId(String id) {  this.id = id; }

  public String getName() { return this.name; }
  public void setName(String name) {
    this.name = name;
  }

  public long getExpiredTime() { return expiredTime; }
  public void setExpiredTime(long expiredTime) { this.expiredTime = expiredTime; }

  public byte[] getData() { return data; }
  public void setData(byte[] data) {
    this.data = data;
  }
}
