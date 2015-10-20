package com.neverwinterdp.scribengin.storage;

import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class Record {
  static public enum Type { DATA, INSTRUCTION } 
  
  private String key ;
  private byte[] data ;
  private Type   type ;
  
  public Record() {} 
  
  public Record(String key, byte[] data) {
    this.key = key ;
    this.data = data ;
    this.type = Type.DATA;
  }
  
  public Record(StorageInstruction ins) {
    this.key = "instruction" ;
    this.data = JSONSerializer.INSTANCE.toBytes(ins);
    this.type = Type.INSTRUCTION;
  }
  
  public String getKey() { return key; }
  public void setKey(String key) { this.key = key; }
  
  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
  
  public Type getType() { return type; }
  public void setType(Type type) { this.type = type; }

  static public Record create(String key, String text) {
    byte[] data = text.getBytes();
    Record dataflowMessage = new Record(key, data) ;
    return dataflowMessage;
  }
  
  static public Record create(String key, byte[] data) {
    Record dataflowMessage = new Record(key, data) ;
    return dataflowMessage;
  }
}
