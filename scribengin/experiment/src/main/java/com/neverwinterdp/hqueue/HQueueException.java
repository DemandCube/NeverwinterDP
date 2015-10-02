package com.neverwinterdp.hqueue;

public class HQueueException extends Exception {
  private static final long serialVersionUID = 1L;

  public HQueueException(String mesg) {
    super(mesg);
  }
  
  public HQueueException(String mesg, Throwable root) {
    super(mesg,root);
  }
}
