package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.ExceptionUtil;

public class TrackingRegistry {
  private Registry registry ;
  private Node appNode ;
  private Node generateReportsNode ;
  private Node generateErrorsNode ;
  private Node validateReportsNode ;
  private String reportPath  ;
  
  public TrackingRegistry(Registry registry, String reportPath, boolean initRegistry) throws RegistryException {
    this.registry = registry;
    this.reportPath = reportPath;
    initRegistry(initRegistry) ;
  }
  
  void initRegistry(boolean create) throws RegistryException {
    if(create) {
      appNode = registry.createIfNotExist(reportPath);
      generateReportsNode = appNode.createDescendantIfNotExists("generate/reports");
      generateErrorsNode  = appNode.createDescendantIfNotExists("generate/errors");
      validateReportsNode = appNode.createDescendantIfNotExists("validate/reports");
    } else {
      appNode = registry.get(reportPath);
      generateReportsNode = appNode.getDescendant("generate/reports");
      generateErrorsNode  = appNode.getDescendant("generate/errors");
      validateReportsNode = appNode.getDescendant("validate/reports");
    }
  }

  public void addGenerateReport(TrackingReport report) throws RegistryException {
    generateReportsNode.createChild(report.getGroupId(), report, NodeCreateMode.PERSISTENT);
  }
  
  public void updateGenerateReport(TrackingReport report) throws RegistryException {
    generateReportsNode.getChild(report.getGroupId()).setData(report);
  }
  
  public List<TrackingReport> getGeneratedReports() throws RegistryException {
    return generateReportsNode.getChildrenAs(TrackingReport.class) ;
  }
  
  public void addGenerateError(String groupId, Throwable error) throws RegistryException {
    String stacktrace = ExceptionUtil.getStackTrace(error);
    generateReportsNode.createChild(groupId, stacktrace, NodeCreateMode.PERSISTENT);
  }
  
  public void saveValidateReport(TrackingReport report) throws RegistryException {
    if(validateReportsNode.hasChild(report.getGroupId())) {
      validateReportsNode.getChild(report.getGroupId()).setData(report);
    } else {
      validateReportsNode.createChild(report.getGroupId(), report, NodeCreateMode.PERSISTENT);
    }
  }

  
  public List<TrackingReport> getValidateReports() throws RegistryException {
    return validateReportsNode.getChildrenAs(TrackingReport.class) ;
  }
}
