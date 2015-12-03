package com.neverwinterdp.scribengin.dataflow.operator;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskContext;
import com.neverwinterdp.registry.task.dedicated.TaskSlotExecutor;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.WorkerService;
import com.neverwinterdp.storage.Record;

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
  public void onShutdown() throws Exception {
    context.commit();
    context.close();
  }
  
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
        if(lastNoMessageTime + 180000 < currentTime) {
          getTaskContext().setComplete();
          context.setComplete();
        }
      } else {
        report.setLastAssignedWithNoMessageProcess(0);
        lastNoMessageTime = -1;
      }
      report.addAccRuntime(currentTime - startTime);
      
      if(context.isComplete() || report.getProcessCount() > 10000 || lastFlushTime + 30000 < currentTime) {
        context.commit();
      }
    } catch(InterruptedException ex) {
      throw ex ;
    } catch(RegistryException error) {
      throw error;
    } catch(Exception error) {
      rollback(error);
    }
  }
  
  void rollback(Exception error) throws Exception {
    context.rollback();
    OperatorTaskReport report = context.getTaskReport();
    report.setAssignedHasErrorCount(report.getAssignedHasErrorCount() + 1);
    workerService.getLogger().error("DataflowTask Error", error);
  }
  
  public void finish() throws Exception {
    OperatorTaskReport report = context.getTaskReport();
    report.setFinishTime(System.currentTimeMillis());
    context.commit();
    context.close();
    DataflowRegistry dflRegistry = workerService.getDataflowRegistry();
    dflRegistry.getTaskRegistry().finish(taskContext, TaskStatus.TERMINATED);
  }
}