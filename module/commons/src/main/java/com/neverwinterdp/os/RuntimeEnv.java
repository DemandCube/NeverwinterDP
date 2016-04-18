package com.neverwinterdp.os;

import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;

/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class RuntimeEnv {
  private String serverName;
  private String vmName;
  private String appDir;
  private String configDir;
  private String logDir;
  private String tmpDir;
  private String workingDir;
  private String dataDir;
  private String[] esConnects;

  public RuntimeEnv(String serverName, String vmName, String defaultAppHome) {
    this.serverName = serverName;
    this.vmName     = vmName;
    if(defaultAppHome == null) defaultAppHome = "build/app";
    appDir     = getSystemProperty("app.home", defaultAppHome) ;
    configDir  = getSystemProperty("app.config.dir", appDir + "/config") ;
    logDir     = getSystemProperty("app.log.dir", appDir + "/logs/" + vmName) ;
    tmpDir     = getSystemProperty("app.tmp.dir", appDir + "/tmp/" + vmName) ;
    workingDir = getSystemProperty("app.working.dir", appDir + "/working/" + vmName) ;
    dataDir    = getSystemProperty("app.data.dir", appDir + "/data/" + vmName) ;
    esConnects = StringUtil.toStringArray(getSystemProperty("app.es.connects", "127.0.0.1:9300"));
  }
  
  public String getServerName() { return this.serverName ; }
  
  public String getVMName() { return this.vmName ; }
  
  public String getAppDir() { return appDir; }

  public void setAppDir(String appDir) { this.appDir = appDir; }

  public String getConfigDir() { return configDir; }

  public void setConfigDir(String configDir) { this.configDir = configDir; }

  public String getLogDir() { return logDir; }
  public void setLogDir(String logDir) { this.logDir = logDir; }

  public String getTmpDir() { return tmpDir; }
  public void setTmpDir(String tmpDir) { this.tmpDir = tmpDir; }
  
  public String getWorkingDir() { return workingDir; }
  public void setWorkingDir(String workingDir) { this.workingDir = workingDir; }

  public String getDataDir() { return dataDir; }
  public void setDataDir(String dataDir) { this.dataDir = dataDir; }
  
  public String[] getEsConnects() { return esConnects; }
  public void setEsConnects(String[] esConnects) { this.esConnects = esConnects; }

  String getSystemProperty(String name, String defaultValue) {
    String value = System.getProperty(name) ;
    if(value != null) return value ;
    return defaultValue ;
  }
  
  public String getFormattedText() {
    TabularFormater formatter = new TabularFormater("Runtime Environment", "") ;
    formatter.addRow("Server Name", serverName);
    formatter.addRow("VM Name", vmName);
    formatter.addRow("App Dir", appDir);
    formatter.addRow("Config Dir", configDir) ;
    formatter.addRow("Log Dir", logDir);
    formatter.addRow("TMP Dir", tmpDir) ;
    formatter.addRow("Working Dir", workingDir);
    formatter.addRow("Data Dir", dataDir);
    return formatter.getFormattedText();
  }
}
