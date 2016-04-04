package com.neverwinterdp.es.sys;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class SysMetricLoggerServiceUnitTest {
  static String WORKING_DIR = "build/working";
  
  private Node node ;
  
  @Before
  public void setup() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("INFO");
    FileUtil.removeIfExist(WORKING_DIR, false);
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          WORKING_DIR + "/elasticsearch/data");
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
    RuntimeEnv runtimeEnv     = new RuntimeEnv("localhost", "testVM", WORKING_DIR);
    OSManagement osManagement = new OSManagement(runtimeEnv);
    SysMetricLoggerService service = new SysMetricLoggerService();
    service.onInit(runtimeEnv, osManagement);
    service.setLogPeriod(3000);
    Thread.sleep(15000);
    service.onDestroy();
  }
}
