package com.neverwinterdp.es.log.sampler;

import com.neverwinterdp.os.DetailThreadInfo;
import com.neverwinterdp.os.FileStoreInfo;
import com.neverwinterdp.os.GCInfo;
import com.neverwinterdp.os.MemoryInfo;
import com.neverwinterdp.os.OSInfo;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.os.ThreadCountInfo;
import com.beust.jcommander.JCommander;
import com.neverwinterdp.es.log.ObjectLoggerService;

public class MetricSampler {
  static public void main(String[] args) throws Exception {
    MetricSamplerConfig config = new MetricSamplerConfig();
    new JCommander(config, args);
    
    
    RuntimeEnv runtimeEnv = new RuntimeEnv(config.vmName, config.vmName, "build/app") ;
    OSManagement osMan = new OSManagement(runtimeEnv) ;

    String bufferDir = runtimeEnv.getAppDir() + "/working/buffer";
    ObjectLoggerService service = new  ObjectLoggerService(new String[] {config.esConnect}, bufferDir, 25000);
    service.add(DetailThreadInfo.class);
    service.add(FileStoreInfo.class);
    service.add(GCInfo.class);
    
    service.add(ThreadCountInfo.class);
    
    service.add(MemoryInfo.class);
    service.add(OSInfo.class);
    
    DetailThreadInfo[] info = osMan.getDetailThreadInfo();
    for(DetailThreadInfo sel : info) {
      service.log(sel.uniqueId(), sel);
    }
    
    for(FileStoreInfo sel : osMan.getFileStoreInfo()) {
      service.log(sel.uniqueId(), sel);
    }
    
    for(GCInfo sel : osMan.getGCInfo()) {
      service.log(sel.uniqueId(), sel);
    }
    
    for(MemoryInfo sel : osMan.getMemoryInfo()) {
      service.log(sel.uniqueId(), sel);
    }
    
    OSInfo osInfo = osMan.getOSInfo();
    service.log(osInfo.uniqueId(), osInfo);
    
    ThreadCountInfo threadCountInfo = osMan.getThreadCountInfo();
    service.log(threadCountInfo.uniqueId(), threadCountInfo);
    System.out.println("Send sample metric done!!!!!!!!!!!!");
    Thread.sleep(10000);
    service.close();
    System.out.println("Push some data done");
  }
}
