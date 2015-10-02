package com.neverwinterdp.scribengin.dataflow.task;

import java.util.Comparator;
import java.util.Set;

public class DataflowTaskConfig {
  final static public Comparator<DataflowTaskConfig> COMPARATOR = new Comparator<DataflowTaskConfig>() {
    @Override
    public int compare(DataflowTaskConfig o1, DataflowTaskConfig o2) {
      return o1.getTaskId().compareTo(o2.getTaskId());
    }
  };
  
  private String                        taskId;
  private String                        operator;
  private String                        input;
  private int                           inputPartitionId;
  private Set<String>                   outputs;
  private String                        scribe;

  public DataflowTaskConfig() {
  }

  public String getTaskId() { return taskId; }
  public void setTaskId(String taskId) { this.taskId = taskId; }

  public String getOperator() { return operator; }
  public void setOperator(String operator) { this.operator = operator; }

  public String getInput() { return input; }
  public void setInput(String input) { this.input = input; }

  public int getInputPartitionId() { return inputPartitionId; }
  public void setInputPartitionId(int id) { this.inputPartitionId = id; }

  public Set<String> getOutputs() { return outputs; }
  public void setOutputs(Set<String> outputs) { this.outputs = outputs; }

  public String getScribe() { return scribe; }
  public void setScribe(String scribe) { this.scribe = scribe; }
}
