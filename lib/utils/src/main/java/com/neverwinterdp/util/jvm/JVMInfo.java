package com.neverwinterdp.util.jvm;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.DateUtil;


public class JVMInfo implements Serializable {
  private String startTime ;
  private String upTime ;
  
  private MemoryInfo memoryInfo;
  private ArrayList<GCInfo> gCInfo ;
  private AppInfo appInfo;
  
  public JVMInfo() {
    startTime = DateUtil.asCompactDateTime(ManagementFactory.getRuntimeMXBean().getStartTime()) ;
    upTime = DateUtil.timeMillisToHumanReadable(ManagementFactory.getRuntimeMXBean().getUptime()) ;
    memoryInfo = new MemoryInfo();
    
    List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans() ; 
    gCInfo = new ArrayList<GCInfo>();
    for(int i = 0; i < gcbeans.size(); i++) {
      GarbageCollectorMXBean gcbean = gcbeans.get(i) ;
      gCInfo.add(new GCInfo(gcbean));
    }
    
    appInfo = new AppInfo();
  }
  
  public String getStartTime() { return this.startTime ; }
  
  public String getUpTime() { return this.upTime ; }
  
  public MemoryInfo getMemoryInfo() { return memoryInfo;}

  public ArrayList<GCInfo> getGarbageCollectorInfo() { 
    return gCInfo; 
  }

  public AppInfo getThreadInfo() { return appInfo; }
}