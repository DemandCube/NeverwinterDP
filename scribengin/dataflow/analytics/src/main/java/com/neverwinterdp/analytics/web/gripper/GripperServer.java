package com.neverwinterdp.analytics.web.gripper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.netty.http.HttpServer;

public class GripperServer {
  @Parameter(names = "--port", description = "The http port")
  private int port         = 7081;
  
  @Parameter(names = "--num-of-workers", description = "The number of the handler thread")
  private int numOfWorkers = 3;
  
  @Parameter(names = "--kafka-zk-connects", description = "Kafka zookeeper connects")
  private String kafkaZKConnects = "127.0.0.1:2181";
  
  @Parameter(names = "--webpage-event-topic", description = "")
  private String webEventTopic = "web.input";
  
  @Parameter(names = "--ads-event-topic", description = "")
  private String adsEventTopic = "ads.input";
  
  private HttpServer server ;
  
  public GripperServer() { }
  
  public GripperServer(String[] args) {
    new JCommander(this, args);
  }
  
  public GripperServer setPort(int port) {
    this.port = port;
    return this;
  }
  
  public GripperServer setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
    return this;
  }
  
  public GripperServer setKafkaZkConnects(String kafkaZkConnects) {
    this.kafkaZKConnects = kafkaZkConnects;
    return this;
  }
  
  public void start() throws Exception {
    server = new HttpServer();
    server.setPort(port).setNumberOfWorkers(numOfWorkers);
    server.add("/rest/client/info.collector", new ClientInfoCollectorHandlerExt(kafkaZKConnects, webEventTopic));
    server.add("/rest/client/ads-event.collector", new AdsEventCollectorHandler(kafkaZKConnects, adsEventTopic));
    server.startAsDeamon();
  }
  
  public void shutdown() {
    server.shutdown() ;
    server = null;
  }
  
  static public void main(String[] args) throws Exception {
    GripperServer server = new GripperServer(args);
    server.start();
    Thread.currentThread().join();
  }
}
