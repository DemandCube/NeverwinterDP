package com.neverwinterdp.scribengin.webui;

import com.neverwinterdp.scribengin.ScribenginClient;

public class DataflowRestRequestHandler extends RestRequestHandler  {
  public DataflowRestRequestHandler(ScribenginClient client) {
    super(client);
    addCommand("active",         DataflowCommand.ListActive.class);
    addCommand("history",        DataflowCommand.ListHistory.class);
    addCommand("info",           DataflowCommand.Info.class);
    addCommand("resume",         DataflowCommand.Resume.class);
    addCommand("stop",           DataflowCommand.Stop.class);
    addCommand("operator.report",DataflowCommand.OperatorReport.class);
    addCommand("master.report",  DataflowCommand.MasterReport.class);
    addCommand("master.kill",    DataflowCommand.MasterKill.class);
    addCommand("worker.report",  DataflowCommand.WorkerReport.class);
    addCommand("worker.kill",    DataflowCommand.WorkerKill.class);
  }
} 