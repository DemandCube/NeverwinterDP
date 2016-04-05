package com.neverwinterdp.scribengin.webui;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.netty.http.HttpServer;
import com.neverwinterdp.netty.http.StaticFileHandler;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;

public class WebuiServer {
  @Parameter(names = "--port", description = "The http port")
  private int port  = 8080;
  
  @Parameter(names = "--num-of-workers", description = "The number of the handler thread")
  private int numOfWorkers = 3;
  
  @Parameter(names = "--www-dir", required=true, description = "www directory")
  private String wwwDir;
 
  @Parameter(names = "--zk-connects", required=true, description = "The zookeeper connect string")
  private String zkConnects;

  @Parameter(names = "--hadoop-master", description = "Hadoop Master")
  private String hadoopMaster;

  private HttpServer server ;
  
  public WebuiServer(String[] args) {
    new JCommander(this, args);
  }
  
  public WebuiServer setPort(int port) {
    this.port = port;
    return this;
  }
  
  public WebuiServer setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
    return this;
  }
  
  public void start() throws Exception {
    server = new HttpServer();
    server.setPort(port).setNumberOfWorkers(numOfWorkers);
    
    StaticFileHandler staticFileHandler = new StaticFileHandler(wwwDir) ;
    server.setDefault(staticFileHandler);

    RegistryConfig registryConfig = new RegistryConfig();
    registryConfig.setConnect(zkConnects);
    Registry registry = registryConfig.newInstance();
    registry.connect();
    VMClient vmClient = null;
    if(hadoopMaster != null) {
      HadoopProperties hadoopProps = new HadoopProperties() ;
      hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
      hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
      //Set up our connection to Scribengin
      vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    } else {
      vmClient = new LocalVMClient(registry);
    }
    ScribenginClient scribenginClient =  new ScribenginClient(vmClient);
    server.add("/rest/vm/:path", new VMRestRequestHandler(scribenginClient));
    server.add("/rest/dataflow/:path", new DataflowRestRequestHandler(scribenginClient));
    server.startAsDeamon();
  }
  
  public void shutdown() {
    server.shutdown() ;
    server = null;
  }
  
  static public void main(String[] args) throws Exception {
    WebuiServer server = new WebuiServer(args);
    server.start();
    Thread.currentThread().join();
  }
}
