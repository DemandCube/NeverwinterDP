package com.neverwinterdp.scribengin.webui;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.analytics.dataflow.AanalyticsDataflowBuilder;
import com.neverwinterdp.analytics.odyssey.generator.OdysseyEventGeneratorServer;
import com.neverwinterdp.analytics.web.gripper.GripperServer;
import com.neverwinterdp.analytics.web.gripper.generator.WebEventGeneratorServer;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.ResponseHandler;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class WebuiIntegrationTest {
  WebuiServer            httpServer;
  GripperServer          gripperServer;
  LocalScribenginCluster localScribenginCluster;
  ScribenginShell        shell;

  @Before
  public void setup() throws Exception {
    String BASE_DIR = "build/working";
    System.setProperty("app.home",   BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    
    shell = localScribenginCluster.getShell();
    
    gripperServer = new GripperServer();
    gripperServer.start();
    
    String[] webuiServerConfig = {
      "--www-dir", "src/main/webapp", 
      "--zk-connects", localScribenginCluster.getKafkaCluster().getZKConnect()
    };
    
    httpServer = new WebuiServer(webuiServerConfig);
    httpServer.start();
  }
  
  @After
  public void teardown() throws Exception {
    httpServer.shutdown();
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    int NUM_OF_ODYSSEY_EVENTS = 100;
    int NUM_OF_WEB_EVENTS     = 200;
    int ALL_EVENTS = NUM_OF_ODYSSEY_EVENTS + NUM_OF_WEB_EVENTS ;
    
    String[] odysseyGeneratorConfig = {
      "--num-of-workers", "1", "--zk-connects", "127.0.0.1:2181", 
      "--topic", "odyssey.input", "--num-of-events", Integer.toString(NUM_OF_ODYSSEY_EVENTS)
    };
    OdysseyEventGeneratorServer odysseyEventGeneratorServer = new OdysseyEventGeneratorServer(odysseyGeneratorConfig); 
    odysseyEventGeneratorServer.start();
    
    String[] webEventGeneratorConfig = {
      "--num-of-pages", Integer.toString(NUM_OF_WEB_EVENTS)
    };
    WebEventGeneratorServer wGeneratorServer = new WebEventGeneratorServer(webEventGeneratorConfig);
    wGeneratorServer.start();
    
    AanalyticsDataflowBuilder dflBuilder = new AanalyticsDataflowBuilder() ;
    Dataflow dfl = dflBuilder.buildDataflow();
    
    try {
      new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForDataflowRunning(60000);
    } catch (Exception ex) {
      shell.execute("registry dump");
      throw ex;
    }
    
    dflBuilder.runMonitor(shell, ALL_EVENTS, false);
    
    System.err.println("**************************************************");
    System.err.println("Start Test Rest Command");
    System.err.println("**************************************************");
    HttpResponseHandler handler = new HttpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", 8080, handler);
    
    assertValidRest(client, handler, "/rest/vm/list-active");
    assertValidRest(client, handler, "/rest/vm/list-history");
    
    assertValidRest(client, handler, "/rest/dataflow/active");
    assertValidRest(client, handler, "/rest/dataflow/history");
    assertValidRest(client, handler, "/rest/dataflow/info?dataflowId=analytics");
    
    assertValidRest(client, handler, "/rest/dataflow/operator.report?dataflowId=analytics");
    assertValidRest(client, handler, "/rest/dataflow/master.report?dataflowId=analytics");
    assertValidRest(client, handler, "/rest/dataflow/worker.report?dataflowId=analytics&groupBy=all");
    assertValidRest(client, handler, "/rest/dataflow/worker.report?dataflowId=analytics&groupBy=active");
    assertValidRest(client, handler, "/rest/dataflow/worker.report?dataflowId=analytics&groupBy=history");
    
    assertValidRest(client, handler, "/rest/dataflow/master.kill?dataflowId=analytics&vmId=analytics-master-0000000001");
    Thread.sleep(10000);
    assertValidRest(client, handler, "/rest/dataflow/worker.kill?dataflowId=analytics&vmId=analytics-worker-0000000001");
    Thread.sleep(100000000);
  }
  
  void assertValidRest(AsyncHttpClient client, HttpResponseHandler handler, String restPath) throws Exception {
    System.out.println("Assert for the rest path: " + restPath);
    client.get(restPath);
    CommandResponse cmdResponse = handler.take();
    Assert.assertNotNull(cmdResponse);
    System.out.println(JSONSerializer.INSTANCE.toString(cmdResponse));
    System.out.println();
  }
  
  static public class HttpResponseHandler implements ResponseHandler {
    private CommandResponse cmdResponse;
    
    @Override
    synchronized public void onResponse(HttpResponse response) {
      HttpResponseStatus status = response.getStatus();
      if(status.code() != 200) {
        System.err.println("Expect http response status = 200, but " + status.code());
        return;
      }
      
      if(response instanceof HttpContent) {
        HttpContent content = (HttpContent) response;
        ByteBuf bBuf = content.content();
        byte[] bytes = new byte[bBuf.readableBytes()];
        bBuf.readBytes(bytes);
        try {
          cmdResponse = JSONSerializer.INSTANCE.fromBytes(bytes, CommandResponse.class);
        } catch(Throwable t) {
          t.printStackTrace();
        }
      }
      notifyAll();
    }
    
    synchronized CommandResponse take() throws InterruptedException {
      if(cmdResponse == null) {
        wait(0);
      }
      CommandResponse ret = cmdResponse;
      cmdResponse = null;
      return ret;
    }
  }
}