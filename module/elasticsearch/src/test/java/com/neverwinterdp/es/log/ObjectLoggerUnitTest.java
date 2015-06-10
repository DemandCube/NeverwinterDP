package com.neverwinterdp.es.log;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.os.DetailThreadInfo;
import com.neverwinterdp.os.OSInfo;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class ObjectLoggerUnitTest {
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
    ObjectLoggerService service = new  ObjectLoggerService(new String[] {"127.0.0.1:9300"}, "build/working/buffer", 25000);
    service.add(DetailThreadInfo.class);
    service.add(OSInfo.class);
    OSManagement osMan = new OSManagement() ;
    DetailThreadInfo[] info = osMan.getDetailThreadInfo();
    for(DetailThreadInfo sel : info) {
      service.log(sel.uniqueId(), sel);
    }
    OSInfo osInfo = osMan.getOSInfo();
    service.log(osInfo.uniqueId(), osInfo);
    Thread.sleep(15000);
  }
}
