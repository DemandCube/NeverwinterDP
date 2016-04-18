package com.neverwinterdp.analytics.gripper;

public class GripperAck {
  private String  objectId;
  private boolean success = false;
  private String  errorMessage ;
  
  public GripperAck() { }
  
  public GripperAck(String objectId) { 
    this.objectId = objectId ; 
    this.success  = true;
  }
  
  public GripperAck(String objectId, String errorMessage) { 
    this.objectId     = objectId ; 
    this.success      = false;
    this.errorMessage = errorMessage;
  }

  public String getObjectId() { return objectId; }
  public void setObjectId(String id) { this.objectId = id; }

  public boolean isSuccess() { return success; }
  public void setSuccess(boolean success) { this.success = success; }

  public String getErrorMessage() { return errorMessage; }
  public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}