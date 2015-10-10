package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorService;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;

public class DataflowTask {
  private DataflowTaskExecutorService executorService;
  private final TaskContext<DataflowTaskDescriptor> taskContext;
  private DataflowTaskDescriptor descriptor ;
  private ScribeAbstract processor;
  private DataflowTaskContext context;
  private boolean interrupt = false;
  private long    startTime = 0;
  
  public DataflowTask(DataflowTaskExecutorService service, TaskContext<DataflowTaskDescriptor> taskContext) throws Exception {
    this.executorService = service;
    this.taskContext     = taskContext;
    this.descriptor      = taskContext.getTaskDescriptor(true);
    Class<ScribeAbstract> scribeType = (Class<ScribeAbstract>) Class.forName(descriptor.getScribe());
    processor = scribeType.newInstance();
  }
  
  public TaskContext<DataflowTaskDescriptor> getTaskContext() { return this.taskContext; }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public boolean isComplete() { return context.isComplete() ; }
  
  public boolean isIterrupted() { return this.interrupt ; }
  
  public void interrupt() { interrupt = true; }
  
  public void init() throws Exception {
    startTime = System.currentTimeMillis();
    DataflowRegistry dRegistry = executorService.getDataflowRegistry();
    DataflowTaskReport report = dRegistry.getTaskReport(descriptor);
    report.incrAssignedCount();
    dRegistry.dataflowTaskReport(descriptor, report);
    context = new DataflowTaskContext(executorService, descriptor, report);
  }
  
  public void execute()  throws InterruptedException {
    DataflowTaskReport report = context.getReport();
    int dataflowMessageCount = 0;
    try {
      while(!interrupt && !context.isComplete() && !context.isEndOfDataStream()) {
        DataflowMessage dataflowMessage = context.nextRecord(5000);
        if(dataflowMessage == null) break ;

        dataflowMessageCount++;
        if(dataflowMessage.getType() == DataflowMessage.Type.INSTRUCTION) {
          DataflowInstruction ins = dataflowMessage.dataAsDataflowInstruction(); 
          processor.process(ins, context);
          if(ins == DataflowInstruction.END_OF_DATASTREAM) {
            context.setComplete() ;
            break;
          }
        } else {
          report.incrProcessCount();
          processor.process(dataflowMessage, context);
        }
      } //end while
      if(dataflowMessageCount == 0) {
        report.setAssignedWithNoMessageProcess(report.getAssignedWithNoMessageProcess() + 1);
        report.setLastAssignedWithNoMessageProcess(report.getLastAssignedWithNoMessageProcess() + 1);
      } else {
        report.setLastAssignedWithNoMessageProcess(0);
      }
      if(context.isEndOfDataStream() || report.getLastAssignedWithNoMessageProcess() >= 3) {
        context.setComplete();
      }
    } catch(InterruptedException ex) {
      //kill simulation
      throw ex ;
    } catch(Throwable t) {
      report.setAssignedHasErrorCount(report.getAssignedHasErrorCount() + 1);
      executorService.getLogger().error("DataflowTask Error", t);
    }
  }
  
  public void suspend() throws Exception {
    saveContext();
    DataflowRegistry dflRegistry = executorService.getDataflowRegistry();
    dflRegistry.dataflowTaskSuspend(taskContext);
  }
  
  public void finish() throws Exception {
    DataflowTaskReport report = context.getReport();
    report.setFinishTime(System.currentTimeMillis());
    saveContext();
    DataflowRegistry dflRegistry = executorService.getDataflowRegistry();
    dflRegistry.dataflowTaskFinish(taskContext);
  }
  
  void saveContext() throws Exception {
    DataflowTaskReport report = context.getReport();
    report.addAccRuntime(System.currentTimeMillis() - startTime);
    context.commit();
    context.close();
  }  
}