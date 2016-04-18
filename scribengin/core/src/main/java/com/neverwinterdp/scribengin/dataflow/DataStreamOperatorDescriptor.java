package com.neverwinterdp.scribengin.dataflow;

import java.util.Comparator;
import java.util.Set;

public class DataStreamOperatorDescriptor {
  final static public Comparator<DataStreamOperatorDescriptor> COMPARATOR = new Comparator<DataStreamOperatorDescriptor>() {
    @Override
    public int compare(DataStreamOperatorDescriptor o1, DataStreamOperatorDescriptor o2) {
      return o1.getTaskId().compareTo(o2.getTaskId());
    }
  };
  
  private String      taskId;
  private String      operatorName;
  private String      input;
  private int         inputPartitionId;
  private Set<String> outputs;
  private String      operator;
  private Set<String> interceptors;
  
  
  public DataStreamOperatorDescriptor() {
  }

  public String getTaskId() { return taskId; }
  public void setTaskId(String taskId) { this.taskId = taskId; }

  public String getOperatorName() { return operatorName; }
  public void setOperatorName(String operator) { this.operatorName = operator; }

  public String getInput() { return input; }
  public void setInput(String input) { this.input = input; }

  public int getInputPartitionId() { return inputPartitionId; }
  public void setInputPartitionId(int id) { this.inputPartitionId = id; }

  public Set<String> getOutputs() { return outputs; }
  public void setOutputs(Set<String> outputs) { this.outputs = outputs; }

  public String getOperator() { return operator; }
  public void setOperator(String operator) { this.operator = operator; }

  public Set<String> getInterceptors() { return interceptors; }
  public void setInterceptors(Set<String> interceptors) {
    this.interceptors = interceptors;
  }
}
