package com.neverwinterdp.scribengin.dataflow.tracking;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class TrackingRegistry {
  private Registry registry ;
  private Node appNode ;
  private Node generateReportsNode ;
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
      validateReportsNode = appNode.createDescendantIfNotExists("validate/reports");
    } else {
      appNode = registry.get(reportPath);
      generateReportsNode = appNode.getDescendant("generate/reports");
      validateReportsNode = appNode.getDescendant("validate/reports");
    }
  }

  public void addGeneratorReport(TrackingMessageReport report) throws RegistryException {
    generateReportsNode.createChild(report.reportName(), report, NodeCreateMode.PERSISTENT);
  }
  
  public void updateGeneratorReport(TrackingMessageReport report) throws RegistryException {
    generateReportsNode.getChild(report.reportName()).setData(report);
  }
  
  public List<TrackingMessageReport> getGeneratorReports() throws RegistryException {
    return generateReportsNode.getChildrenAs(TrackingMessageReport.class) ;
  }
  
  public void addValidatorReport(TrackingMessageReport report) throws RegistryException {
    validateReportsNode.createChild(report.reportName(), report, NodeCreateMode.PERSISTENT);
  }
  
  public void updateValidatorReport(TrackingMessageReport report) throws RegistryException {
    validateReportsNode.getChild(report.reportName()).setData(report);
  }
  
  public void saveValidatorReport(TrackingMessageReport report) throws RegistryException {
    if(validateReportsNode.hasChild(report.reportName())) {
      validateReportsNode.getChild(report.reportName()).setData(report);
    } else {
      validateReportsNode.createChild(report.reportName(), report, NodeCreateMode.PERSISTENT);
    }
  }

  public List<TrackingMessageReport> getValidatorReports() throws RegistryException {
    return validateReportsNode.getChildrenAs(TrackingMessageReport.class) ;
  }
}
