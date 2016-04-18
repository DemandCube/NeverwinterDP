package com.neverwinterdp.scribengin.webui;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.JSONSerializer;

public class CommandResponse {
  private String  command;
  private String  error;
  private String  resultMappingType;
  private String  result;
  
  public CommandResponse() { } ;
  
  public CommandResponse(String cmdName) { 
    this.command = cmdName;
  } ;
  
  public String getCommand() { return command; }
  public void setCommand(String cmd) { this.command = cmd; }
  
  public String getError() { return error; }
  public void setError(String error) { this.error = error; }
  
  public String getResultMappingType() { return resultMappingType; }
  public void setResultMappingType(String resultMappingType) { this.resultMappingType = resultMappingType; }
  
  public String getResult() { return result; }
  public void setResult(String result) {
    this.result = result;
  }
  
  @JsonIgnore
  public void setResultAs(Object result) {
    this.resultMappingType = result.getClass().getName();
    this.result = JSONSerializer.INSTANCE.toString(result);
  }
  
  @JsonIgnore
  public <T> T getResultAs(Class<T> type) {
    return JSONSerializer.INSTANCE.fromString(result, type);
  }
}
