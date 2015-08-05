package com.neverwinterdp.scribengin.shell;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.txevent.TXEventNotificationWatcher;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.test.ExecuteLog;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.TabularFormater;

public class StartStopDataflowExecutor extends Executor {
  @Parameter(names = "--wait-before-start", description = "The command should repeat in this failurePeriod of time")
  public long waitBeforeSimulateFailure = -1;
  
  @Parameter(names = "--failure-period", description = "The command should repeat in this failurePeriod of time")
  public long failurePeriod = 10000;
  
  @Parameter(names = "--print-summary", description = "Enable to dump the registry at the end")
  protected boolean printSummary = false;
  
  @Parameter(names = "--max-execution", description = "The maximum number of start/stop/resume execution")
  public int maxExecution = 3;

  String dataflowId ;
  private DataflowClient   dflClient ;
  private List<ExecuteLog> executeLogs = new ArrayList<>();
  private int stopCount                = 0; 
  private int stopCompleteCount        = 0; 
  private int resumeCount              = 0 ;
  private int resumeCompleteCount      = 0;
  
  public StartStopDataflowExecutor(ScribenginShell shell, String dataflowId) {
    super(shell);
    this.dataflowId = dataflowId ;
  }
  
  @Override
  public void run() {
    try {
      execute(shell);
    } catch(Exception ex) {
      ex.printStackTrace();
      try {
        shell.execute("registry dump");
        shell.execute("dataflow info --dataflow-id " + dataflowId);
      } catch(Exception error) {
        error.printStackTrace();
      }
    }
  }
  
  void execute(ScribenginShell shell) throws Exception {
    Notifier notifier       = null ;
    ScribenginClient scribenginClient = shell.getScribenginClient() ;
    dflClient = scribenginClient.getDataflowClient(dataflowId, 180000);
    String notifierPath = "/scribengin/failure-simulation/" + dataflowId + "/start-stop-resume";
    Registry registry = scribenginClient.getRegistry() ;

    if(waitBeforeSimulateFailure > 0) Thread.sleep(waitBeforeSimulateFailure);
    
    int count = 0 ;
    while(count < maxExecution) {
      DataflowLifecycleStatus dataflowStatus = dflClient.getStatus();
      if(dataflowStatus != DataflowLifecycleStatus.RUNNING) break;
      count++ ;
      notifier = new Notifier(registry, notifierPath, "simulation-" + (count + 1));
      notifier.initRegistry();
      
      notifier.info("wait-before-stop", "Wait " + failurePeriod + "ms before stop the dataflow");
      
      notifier.info("check-dataflow-status", "The current dataflow status is " + dataflowStatus);
      if(dataflowStatus == DataflowLifecycleStatus.FINISH || dataflowStatus == DataflowLifecycleStatus.TERMINATED) {
        notifier.info("terminate", "Terminate the failure simulation, the dataflow status is " + dataflowStatus);
        break;
      }
      stopCount++ ;
      DataflowEvent stopEvent = stopCount % 2 == 0 ? DataflowEvent.PAUSE : DataflowEvent.STOP;
      notifier.info("before-stop", "Before execute stop with the event" + stopEvent);
      ExecuteLog stopExecuteLog = doStop(dflClient, stopEvent) ;
      notifier.info("after-stop", stopExecuteLog.getFormatText());
      executeLogs.add(stopExecuteLog);
      shell.console().println(stopExecuteLog.getFormatText());
      if(!stopExecuteLog.isSuccess()) break;
      stopCompleteCount++ ;

      notifier.info("sleep-before-resume", "Sleep " + failurePeriod +"ms before resume");
      Thread.sleep(failurePeriod);

      resumeCount++ ;
      notifier.info("before-resume", "Before resume");
      ExecuteLog resumeExecuteLog = doResume(dflClient) ;
      notifier.info("after-resume", resumeExecuteLog.getFormatText());
      executeLogs.add(resumeExecuteLog);
      shell.console().println(resumeExecuteLog.getFormatText());
      if(!resumeExecuteLog.isSuccess()) break;
      resumeCompleteCount++ ;
    }

    if(printSummary) printSummary(shell);
  }
  
  void printSummary(ScribenginShell shell) throws Exception {
    TabularFormater formater = new TabularFormater("#", "Description", "Duration", "Success") ;
    formater.setTitle("Start/Pause/Stop/Resume Test Summary");
    for(int i = 0; i < executeLogs.size(); i++) {
      ExecuteLog sel = executeLogs.get(i) ;
      Object[] cells = {
        (i + 1), sel.getDescription(), (sel.getStop() - sel.getStart()) + "ms", sel.isSuccess() 
      };
      formater.addRow(cells);
    }
    shell.console().println(formater.getFormatText());
    shell.console().println(
        "stop = " + stopCount +", stop complete = " + stopCompleteCount + ", " +
        "resume = " + resumeCount + ", resume complete = " + resumeCompleteCount);
  }
  
  ExecuteLog doStop(DataflowClient dflClient, DataflowEvent stopEvent) throws Exception {
    DataflowRegistry dflRegistry = dflClient.getDataflowRegistry();
    ExecuteLog executeLog = new ExecuteLog("Stop the dataflow with the event " + stopEvent) ;
    executeLog.start(); 
    
    DataflowLifecycleStatus expectStatus = DataflowLifecycleStatus.PAUSE;
    if(stopEvent == DataflowEvent.STOP) {
      expectStatus = DataflowLifecycleStatus.STOP;
    }
    System.err.println("Client: start request stop, event = " + stopEvent + ", expect status = " + expectStatus);
    TXEventNotificationWatcher watcher = dflClient.broadcastDataflowEvent(stopEvent);
    int countNotification = watcher.waitForNotifications(1, 60000);
    watcher.complete();
    if(!dflRegistry.waitForDataflowStatus(expectStatus, 60000)) {
      executeLog.setSuccess(false);
      executeLog.addLog("Fail to wait for the dataflow" + expectStatus + " status");
    }
    executeLog.stop();
    System.err.println("Client: finish request stop");
    System.err.println("  Count notification  = " + countNotification);
    System.err.println("  Log  : " + JSONSerializer.INSTANCE.toString(executeLog));
    return executeLog;
  }
  
  ExecuteLog doResume(DataflowClient dflClient) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Resume the dataflow") ;
    executeLog.start();
    System.err.println("Client: start request resume...");

    DataflowRegistry dflRegistry = dflClient.getDataflowRegistry();
    TXEventNotificationWatcher watcher = dflClient.broadcastDataflowEvent(DataflowEvent.RESUME);
    int countNotification = watcher.waitForNotifications(1, 60000);
    watcher.complete();
    if(!dflRegistry.waitForDataflowStatus(DataflowLifecycleStatus.RUNNING, 60000)) {
      executeLog.setSuccess(false);
      executeLog.addLog("Fail to wait for the dataflow RUNNING status");
    }
    executeLog.stop();
    System.err.println("Client: finish request resume");
    System.err.println("Client: finish request stop");
    System.err.println("  Count notification  = " + countNotification);
    System.err.println("  Log  : " + JSONSerializer.INSTANCE.toString(executeLog));
    return executeLog ;
  }
}
