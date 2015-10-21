package com.neverwinterdp.scribengin.dataflow.operator;

import java.util.Comparator;

public class OperatorTaskReport {
  final static public Comparator<OperatorTaskReport> COMPARATOR = new Comparator<OperatorTaskReport>() {
    @Override
    public int compare(OperatorTaskReport o1, OperatorTaskReport o2) {
      return o1.getTaskId().compareTo(o2.getTaskId());
    }
  };
  
  private String taskId ;
  private String operatorName;
  
  private long   startTime  ;
  private long   finishTime ;
  
  private int    assignedCount    ;
  private int    assignedHasErrorCount ;
  private int    assignedWithNoMessageProcess ;
  private int    lastAssignedWithNoMessageProcess ;
  
  private long   lastCommitTime ;
  private int    commitCount;
  private int    commitFailCount ;
  
  private long   processCount ;
  private long   accCommitProcessCount;
  private long   accRuntime ;
  
  public OperatorTaskReport() {} 
  
  public OperatorTaskReport(String taskId, String operatorName) {
    this.taskId = taskId ;
    this.operatorName = operatorName;
    this.startTime = System.currentTimeMillis();
  }
  
  public String getTaskId() { return taskId; }
  public void setTaskId(String taskId) { this.taskId = taskId; }
  
  public String getOperatorName() { return operatorName; }
  public void setOperatorName(String operatorName) { this.operatorName = operatorName; }

  public long getStartTime() { return startTime; }
  public void setStartTime(long startTime) { this.startTime = startTime; }
  
  public long getFinishTime() { return finishTime; }
  public void setFinishTime(long finishTime) { this.finishTime = finishTime; }

  public int getAssignedCount() { return assignedCount; }
  public void setAssignedCount(int assignedCount) {
    this.assignedCount = assignedCount;
  }
  
  public void incrAssignedCount() { 
    this.assignedCount++ ;
  }
  
  public int getAssignedHasErrorCount() { return assignedHasErrorCount; }
  public void setAssignedHasErrorCount(int assignedHasErrorCount) {
    this.assignedHasErrorCount = assignedHasErrorCount;
  }

  public int getAssignedWithNoMessageProcess() { return assignedWithNoMessageProcess; }
  public void setAssignedWithNoMessageProcess(int assignedWithNoMessageProcess) {
    this.assignedWithNoMessageProcess = assignedWithNoMessageProcess;
  }
  
  public int getLastAssignedWithNoMessageProcess() { return lastAssignedWithNoMessageProcess; }
  public void setLastAssignedWithNoMessageProcess(int lastAssignedWithNoMessageProcess) {
    this.lastAssignedWithNoMessageProcess = lastAssignedWithNoMessageProcess;
  }
  
  public long getLastCommitTime() { return lastCommitTime; }
  public void setLastCommitTime(long lastCommitTime) { this.lastCommitTime = lastCommitTime; }

  public int getCommitCount() { return commitCount; }
  public void setCommitCount(int commitCount) {
    this.commitCount = commitCount;
  }
  
  public int getCommitFailCount() { return commitFailCount; }
  public void setCommitFailCount(int commitFailCount) {
    this.commitFailCount = commitFailCount;
  }

  public long getProcessCount() { return processCount; }
  public void setProcessCount(long processCount) { this.processCount = processCount; }
  
  public void incrProcessCount() { processCount++ ; }
  
  public long getAccCommitProcessCount() { return accCommitProcessCount; }
  public void setAccCommitProcessCount(long commitProcessCount) { this.accCommitProcessCount = commitProcessCount; }
  
  public long getAccRuntime() { return accRuntime; }
  public void setAccRuntime(long runtime) { accRuntime = runtime; }
  
  public void addAccRuntime(long amount) {
    accRuntime += amount;
  }

  public long durationTime() {
    if(finishTime > 0) return finishTime - startTime;
    return System.currentTimeMillis() - startTime;
  }
  
  public void updateCommit() { 
    commitCount++ ;
    lastCommitTime = System.currentTimeMillis();
    accCommitProcessCount += processCount ;
    processCount = 0;
  }
}
