package com.neverwinterdp.scribengin.dataflow.operator;

import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskContext;
import com.neverwinterdp.registry.task.dedicated.TaskSlotExecutor;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.WorkerService;
import com.neverwinterdp.scribengin.storage.Record;

public class OperatorTaskSlotExecutor extends TaskSlotExecutor<OperatorTaskConfig>{
  private WorkerService                            workerService;
  private DedicatedTaskContext<OperatorTaskConfig> taskContext;
  private OperatorTaskConfig                       operatorTaskConfig;
  private Operator                                 operator;
  private OperatorContext                          context;
  
  private long                                     startTime = 0;
  private long                                     lastFlushTime = System.currentTimeMillis();
  private long                                     lastNoMessageTime ;
  
  public OperatorTaskSlotExecutor(WorkerService service, DedicatedTaskContext<OperatorTaskConfig> taskContext) throws Exception {
    super(taskContext);
    this.workerService = service;
    this.taskContext     = taskContext;
    this.operatorTaskConfig      = taskContext.getTaskDescriptor(false);
    Class<Operator> opType = (Class<Operator>) Class.forName(operatorTaskConfig.getOperator());
    operator = opType.newInstance();
    
    startTime = System.currentTimeMillis();
    DataflowRegistry dRegistry = workerService.getDataflowRegistry();
    OperatorTaskReport report = dRegistry.getTaskRegistry().getTaskReport(operatorTaskConfig);
    report.incrAssignedCount();
    dRegistry.getTaskRegistry().save(operatorTaskConfig, report);
    context = new OperatorContext(workerService, operatorTaskConfig, report);
    dRegistry.getTaskRegistry().save(operatorTaskConfig, report);
  }
  
  public DedicatedTaskContext<OperatorTaskConfig> getTaskContext() { return this.taskContext; }
  
  public OperatorTaskConfig getDescriptor() { return operatorTaskConfig ; }
  
  public boolean isComplete() { return context.isComplete() ; }
  
  @Override
  public void executeSlot() throws Exception {
    startTime = System.currentTimeMillis();
    OperatorTaskReport report = context.getTaskReport();
    int recCount = 0;
    try {
      while(!isInterrupted()) {
        Record record = context.nextRecord(1000);
        if(record == null) break ;

        recCount++;
        report.incrProcessCount();
        operator.process(context, record);
      } //end while
      
      long currentTime = System.currentTimeMillis();
      if(recCount == 0) {
        if(lastNoMessageTime < 0) lastNoMessageTime = currentTime;
        report.setAssignedWithNoMessageProcess(report.getAssignedWithNoMessageProcess() + 1);
        report.setLastAssignedWithNoMessageProcess(report.getLastAssignedWithNoMessageProcess() + 1);
        if(lastNoMessageTime + 15000 < currentTime) {
          getTaskContext().setComplete();
          context.setComplete();
        }
      } else {
        report.setLastAssignedWithNoMessageProcess(0);
        lastNoMessageTime = -1;
      }
      report.addAccRuntime(currentTime - startTime);
      
      if(context.isComplete() || report.getProcessCount() > 5000 || lastFlushTime + 10000 < currentTime) {
        updateContext();
      }
    } catch(InterruptedException ex) {
      //kill simulation
      throw ex ;
    } catch(Throwable t) {
      report.setAssignedHasErrorCount(report.getAssignedHasErrorCount() + 1);
      workerService.getLogger().error("DataflowTask Error", t);
      t.printStackTrace();
    }
  }
  
  public void suspend() throws Exception {
    saveContext();
    DataflowRegistry dflRegistry = workerService.getDataflowRegistry();
    dflRegistry.getTaskRegistry().suspend(taskContext);
  }
  
  public void finish() throws Exception {
    OperatorTaskReport report = context.getTaskReport();
    report.setFinishTime(System.currentTimeMillis());
    saveContext();
    DataflowRegistry dflRegistry = workerService.getDataflowRegistry();
    dflRegistry.getTaskRegistry().finish(taskContext, TaskStatus.TERMINATED);
  }
  
  void saveContext() {
    try {
      OperatorTaskReport report = context.getTaskReport();
      report.addAccRuntime(System.currentTimeMillis() - startTime);
      context.commit();
      context.close();
    } catch(Exception ex) {
      workerService.getLogger().error("Cannot save the executor context due to the error: " + ex);
    }
  }
  
  void updateContext() throws Exception {
    OperatorTaskReport report = context.getTaskReport();
    context.commit();
  }
}