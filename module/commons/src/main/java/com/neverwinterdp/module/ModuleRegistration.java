package com.neverwinterdp.module;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;

public class ModuleRegistration implements Serializable {
  static public enum RunningStatus { INSTALLED, START, STOP, UNINSTALLED }
  
  static public enum InstallStatus { AVAILABLE, INSTALLED }
  
  private String moduleName;
  private String configureClass;
  private boolean autostart = false ;
  private boolean autoInstall = false ;
  private InstallStatus installStatus =  InstallStatus.AVAILABLE ;
  private RunningStatus runningStatus = RunningStatus.UNINSTALLED ;

  public String getModuleName() { return moduleName; }

  public void setModuleName(String moduleName) { this.moduleName = moduleName; }
  public String getConfigureClass() { return configureClass; }

  public void setConfigureClass(String configureClass) { this.configureClass = configureClass; }

  public boolean isAutostart() { return autostart; }
  public void setAutostart(boolean autostart) { this.autostart = autostart;}

  public boolean isAutoInstall() { return autoInstall; }
  public void setAutoInstall(boolean autoInstall) { this.autoInstall = autoInstall; }

  public InstallStatus getInstallStatus() { return installStatus; }
  public void setInstallStatus(InstallStatus installStatus) { this.installStatus = installStatus; }

  public RunningStatus getRunningStatus() { return runningStatus; }
  public void setRunningStatus(RunningStatus runningStatus) { this.runningStatus = runningStatus; }
  
  static public void loadByAnnotation(Map<String, ModuleRegistration> holder, String ... packages) {
    for(String pkg : packages) {
      Reflections reflections = new Reflections(pkg);
      Set<Class<?>> annotateds = reflections.getTypesAnnotatedWith(ModuleConfig.class);
      for(Class<?> clazz : annotateds) {
        ModuleRegistration mreg = new ModuleRegistration() ;
        ModuleConfig config = clazz.getAnnotation(ModuleConfig.class) ;
        mreg.setModuleName(config.name());
        mreg.setAutoInstall(config.autoInstall());
        mreg.setAutostart(config.autostart());
        mreg.setConfigureClass(clazz.getName());
        holder.put(mreg.getModuleName(), mreg) ;
      }
    }
  }
  
  static public ModuleRegistration loadByClass(String className) throws Exception {
    Class<?> clazz = Class.forName(className) ;
    ModuleRegistration mreg = new ModuleRegistration() ;
    ModuleConfig config = clazz.getAnnotation(ModuleConfig.class) ;
    if(config != null) {
      mreg.setModuleName(config.name());
      mreg.setAutoInstall(config.autoInstall());
      mreg.setAutostart(config.autostart());
    } else {
      mreg.setModuleName(clazz.getSimpleName());
    }
    mreg.setConfigureClass(clazz.getName());
    return mreg ;
  }
}