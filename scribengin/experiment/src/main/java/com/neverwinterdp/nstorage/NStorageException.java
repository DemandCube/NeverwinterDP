package com.neverwinterdp.nstorage;

public class NStorageException extends Exception {
  private static final long serialVersionUID = 1L;

  public NStorageException(String mesg) {
    super(mesg);
  }
  
  public NStorageException(String mesg, Throwable root) {
    super(mesg,root);
  }
}
