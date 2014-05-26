package com.neverwinterdp.server.command;

public class ServerCommandResult <T> {
  private T result ;
  private Exception error ;
  
  public T getResult() {
    return result;
  }

  public void setResult(T result) {
    this.result = result;
  }
  
  public boolean hasError() { return error != null ; }
  
  public Exception getError() {
    return error;
  }
  
  public void setError(Exception error) {
    this.error = error;
  }
}
