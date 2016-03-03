package com.neverwinterdp.message;

import com.neverwinterdp.storage.StorageInstruction;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class Message {
  private String          key;
  private byte[]          data;
  private MessageType     type;
  private MessageTracking messageTracking;

  public Message() {} 
  
  public Message(String key, byte[] data) {
    this.key = key ;
    this.data = data ;
    this.type = MessageType.DATA;
  }
  
  public String getKey() { return key; }
  public void setKey(String key) { this.key = key; }
  
  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
  
  public MessageType getType() { return type; }
  public void setType(MessageType type) { this.type = type; }

  public MessageTracking getMessageTracking() { return messageTracking; }
  public void setMessageTracking(MessageTracking messageTracking) { 
    this.messageTracking = messageTracking;
  }

  static public Message create(String key, String text) {
    byte[] data = text.getBytes();
    Message dataflowMessage = new Message(key, data) ;
    return dataflowMessage;
  }
  
  static public Message create(String key, byte[] data) {
    Message message = new Message(key, data) ;
    return message;
  }
}
