package com.neverwinterdp.storage.es;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.es.sink.ESSink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.Log4jRecord;

public class ESSinkUnitTest {
  Node node ;

  @Before
  public void setUp() throws Exception {
    FileUtil.removeIfExist("build/elasticsearch", false);
    FileUtil.removeIfExist("build/buffer", false);
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          "build/elasticsearch/data");
    nb.getSettings().put("node.name",          "elasticsearch-1");
    nb.getSettings().put("transport.tcp.port", "9300");
    node = nb.node();
  }
  
  @After
  public void tearDown() throws Exception {
    node.close();
  }

  @Test
  public void testSource() throws Exception {
    ESSink sink = new ESSink(new String[] {"127.0.0.1:9300"}, "log4j", Log4jRecord.class) ;
    SinkPartitionStream stream = sink.getPartitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
      Log4jRecord log4jRec = new Log4jRecord() ;
      log4jRec.withTimestamp(System.currentTimeMillis());
      log4jRec.setLevel("INFO");
      log4jRec.setMessage("message " + i);
      Message dataflowMessage = new Message("key-" + i, JSONSerializer.INSTANCE.toBytes(log4jRec));
      writer.append(dataflowMessage);
    }
    writer.close();
  }
}
