package com.neverwinterdp.es.log;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.os.DetailThreadInfo;
import com.neverwinterdp.os.FileStoreInfo;
import com.neverwinterdp.os.GCInfo;
import com.neverwinterdp.os.MemoryInfo;
import com.neverwinterdp.os.OSInfo;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.os.ThreadCountInfo;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class ObjectLoggerServiceUnitTest {
  private Node node ;
  
  @Before
  public void setup() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("INFO");
    FileUtil.removeIfExist("build/working", false);
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          "build/working/elasticsearch/data");
    nb.getSettings().put("node.name",          "elasticsearch-1");
    nb.getSettings().put("transport.tcp.port", "9300");

    node = nb.node();
  }
  
  @After
  public void after() {
    node.close();
  }
  
  @Test
  public void testService() throws Exception {
    RuntimeEnv runtimeEnv = new RuntimeEnv("localhost", "localhost", "build/app") ;
    OSManagement osMan = new OSManagement(runtimeEnv) ;

    ObjectLoggerService service = new  ObjectLoggerService(new String[] {"127.0.0.1:9300"}, "build/working/buffer", 25000);
    service.add(DetailThreadInfo.class);
    service.add(FileStoreInfo.class);
    service.add(GCInfo.class);
    service.add(MemoryInfo.class);
    service.add(OSInfo.class);
    service.add(ThreadCountInfo.class);
    
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
    
    Thread.sleep(15000);
  }
}