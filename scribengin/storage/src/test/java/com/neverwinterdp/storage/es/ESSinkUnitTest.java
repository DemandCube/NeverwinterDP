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
    ESStorageConfig esStorageConfig = new ESStorageConfig("test", "log4j", new String[] { "127.0.0.1:9300" }, Log4jRecord.class);
    ESStorage esStorage = new ESStorage(esStorageConfig);
    ESSink sink = esStorage.getSink() ;
    SinkPartitionStream stream = sink.getPartitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
      Log4jRecord log4jRec = new Log4jRecord() ;
      log4jRec.withTimestamp(System.currentTimeMillis());
      log4jRec.setLevel("INFO");
      log4jRec.setMessage("message " + i);
      Message message = new Message("key-" + i, JSONSerializer.INSTANCE.toBytes(log4jRec));
      writer.append(message);
    }
    writer.close();
  }
}
