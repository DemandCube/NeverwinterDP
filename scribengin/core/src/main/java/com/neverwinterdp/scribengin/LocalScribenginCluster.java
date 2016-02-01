package com.neverwinterdp.scribengin;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Properties;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class LocalScribenginCluster {
  private String baseDir ;
  private KafkaCluster kafkaCluster;
  private Node esNode ;
  private ScribenginClusterBuilder scribenginClusterBuilder;
  private ScribenginShell shell;
  
  public LocalScribenginCluster(String baseDir) throws Exception {
    this.baseDir = baseDir;
    kafkaCluster = new KafkaCluster(baseDir, 1, 1);
    kafkaCluster.setNumOfPartition(5);
  }
  
  public KafkaCluster getKafkaCluster() { return this.kafkaCluster; }

  public ScribenginClient getScribenginClient() {
    return scribenginClusterBuilder.getScribenginClient();
  }
  
  public ScribenginShell getShell() { return shell; }
  
  public void clean() throws Exception {
    FileUtil.removeIfExist(baseDir, false);
  }
  
  public void useLog4jConfig(String resPath) throws Exception {
    Properties log4jProps = new Properties() ;
    log4jProps.load(IOUtil.loadRes(resPath));
    log4jProps.setProperty("log4j.rootLogger", "INFO, file");
    LoggerFactory.log4jConfigure(log4jProps);
  }
  
  public void start() throws Exception {
    VMClusterBuilder.h1("Start Elasticsearch");
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name",       "neverwinterdp");
    nb.getSettings().put("path.data",          baseDir + "/elasticsearch/data");
    nb.getSettings().put("node.name",          "elasticsearch-1");
    nb.getSettings().put("transport.tcp.port", "9300");
    esNode = nb.node();
    
    VMClusterBuilder.h1("Start kafka cluster");
    kafkaCluster.start();
    Thread.sleep(1000);
    
    VMClusterBuilder.h1("Start vm-master");
    scribenginClusterBuilder = new ScribenginClusterBuilder(new VMClusterBuilder(null, new LocalVMClient()));
    scribenginClusterBuilder.start();
    
    shell = new ScribenginShell(getScribenginClient());
  }
  
  public void shutdown() throws Exception {
    scribenginClusterBuilder.shutdown();
    kafkaCluster.shutdown();
    esNode.stop();
    esNode.close();
  }
}