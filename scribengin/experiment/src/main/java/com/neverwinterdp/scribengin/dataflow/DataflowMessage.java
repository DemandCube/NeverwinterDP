package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class DataflowMessage {
  static public enum Type { DATA, INSTRUCTION } 
  private String key ;
  private byte[] data ;
  private Type   type ;
  
  public DataflowMessage() {} 
  
  public DataflowMessage(String key, byte[] data) {
    this.key = key ;
    this.data = data ;
    this.type = Type.DATA;
  }
  
  public DataflowMessage(DataflowInstruction ins) {
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

  public DataflowInstruction dataAsDataflowInstruction() {
    return JSONSerializer.INSTANCE.fromBytes(data, DataflowInstruction.class);
  }
  
  static public DataflowMessage create(String key, String text) {
    byte[] data = text.getBytes();
    DataflowMessage dataflowMessage = new DataflowMessage(key, data) ;
    return dataflowMessage;
  }
  
  static public DataflowMessage create(String key, byte[] data) {
    DataflowMessage dataflowMessage = new DataflowMessage(key, data) ;
    return dataflowMessage;
  }
}
