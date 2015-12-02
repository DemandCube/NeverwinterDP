package com.neverwinterdp.scribengin.dataflow.operator;

import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskContext;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.WorkerService;
import com.neverwinterdp.storage.Record;

public class OperatorTask {
  private WorkerService                            workerService;
  private DedicatedTaskContext<OperatorTaskConfig> taskContext;
  private OperatorTaskConfig                       operatorTaskConfig;
  private Operator                                 operator;
  private OperatorContext                          context;
  private boolean                                  interrupt = false;
  private long                                     startTime = 0;

  public OperatorTask(WorkerService service, DedicatedTaskContext<OperatorTaskConfig> taskContext) throws Exception {
    this.workerService = service;
    this.taskContext     = taskContext;
    this.operatorTaskConfig      = taskContext.getTaskDescriptor(false);
    Class<Operator> opType = (Class<Operator>) Class.forName(operatorTaskConfig.getOperator());
    operator = opType.newInstance();
  }
  
  public DedicatedTaskContext<OperatorTaskConfig> getTaskContext() { return this.taskContext; }
  
  public OperatorTaskConfig getDescriptor() { return operatorTaskConfig ; }
  
  public boolean isComplete() { return context.isComplete() ; }
  
  public boolean isIterrupted() { return this.interrupt ; }
  
  public void interrupt() { interrupt = true; }
  
  public void init() throws Exception {
    startTime = System.currentTimeMillis();
    DataflowRegistry dRegistry = workerService.getDataflowRegistry();
    OperatorTaskReport report = dRegistry.getTaskRegistry().getTaskReport(operatorTaskConfig);
    report.incrAssignedCount();
    dRegistry.getTaskRegistry().save(operatorTaskConfig, report);
    context = new OperatorContext(workerService, operatorTaskConfig, report);
  }
  
  public void execute()  throws InterruptedException {
    OperatorTaskReport report = context.getTaskReport();
    int recCount = 0;
    try {
      while(!interrupt && !context.isComplete()) {
        Record record = context.nextRecord(1000);
        if(record == null) break ;

        recCount++;
        report.incrProcessCount();
        operator.process(context, record);
      } //end while
      if(recCount == 0) {
        report.setAssignedWithNoMessageProcess(report.getAssignedWithNoMessageProcess() + 1);
        report.setLastAssignedWithNoMessageProcess(report.getLastAssignedWithNoMessageProcess() + 1);
      } else {
        report.setLastAssignedWithNoMessageProcess(0);
      }
      if(report.getLastAssignedWithNoMessageProcess() >= 3) {
        context.setComplete();
      }
    } catch(InterruptedException ex) {
      //kill simulation
      throw ex ;
    } catch(Throwable t) {
      report.setAssignedHasErrorCount(report.getAssignedHasErrorCount() + 1);
      workerService.getLogger().error("DataflowTask Error", t);
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
}