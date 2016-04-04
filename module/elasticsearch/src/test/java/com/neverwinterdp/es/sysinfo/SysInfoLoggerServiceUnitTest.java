package com.neverwinterdp.es.sysinfo;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Map;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.es.ESObjectClientUnitTest.Record;
import com.neverwinterdp.es.sysinfo.SysInfo;
import com.neverwinterdp.es.sysinfo.SysInfoLoggerService;
import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class SysInfoLoggerServiceUnitTest {
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
    SysInfoLoggerService service = new SysInfoLoggerService();
    service.onInit(runtimeEnv);
    service.setLogPeriod(3000);
    Thread.sleep(15000);
    service.onDestroy();
    
    ESClient esclient = new ESClient(new String[] { "127.0.0.1:9300" });
    ESObjectClient<SysInfo> esObjecclient = new ESObjectClient<SysInfo>(esclient, "neverwinterdp-sys-info", SysInfo.class) ;
    System.out.println("SysMetric records = " + esObjecclient.count(termQuery("host", "testVM")));
    System.out.println("SysMetric Heap_Memory = " + esObjecclient.count(termQuery("metric.mem.name", "Heap")));
  }
}
