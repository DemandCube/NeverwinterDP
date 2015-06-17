package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.ExceptionUtil;

public class LogSampleRegistry {
  final static public String APP_PATH = "/apps/log-sample" ;

  private Registry registry ;
  private Node appNode ;
  private Node generateReportsNode ;
  private Node generateErrorsNode ;
  private Node validateReportsNode ;
  
  public LogSampleRegistry(Registry registry) throws RegistryException {
    this.registry = registry;
    onInit() ;
  }
  
  public void onInit() throws RegistryException {
    this.appNode = registry.createIfNotExist(APP_PATH);
    
    this.generateReportsNode = appNode.createDescendantIfNotExists("generate/reports");
    this.generateErrorsNode  = appNode.createDescendantIfNotExists("generate/errors");
    
    this.validateReportsNode = appNode.createDescendantIfNotExists("validate/reports");
  }
  
  public void addGenerateReport(LogMessageReport report) throws RegistryException {
    generateReportsNode.createChild(report.getGroupId(), report, NodeCreateMode.PERSISTENT);
  }
  
  public void addGenerateError(String groupId, Throwable error) throws RegistryException {
    String stacktrace = ExceptionUtil.getStackTrace(error);
    generateReportsNode.createChild(groupId, stacktrace, NodeCreateMode.PERSISTENT);
  }
  
  
  public void addValidateReport(LogMessageReport report) throws RegistryException {
    validateReportsNode.createChild(report.getGroupId(), report, NodeCreateMode.PERSISTENT);
  }
  
}
