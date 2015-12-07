package com.neverwinterdp.es;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.util.JSONSerializer;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
@Singleton
public class ElasticSearchService {
  private Logger logger = LoggerFactory.getLogger(ElasticSearchService.class);
  
  @Inject @Named("esProperties")
  private Map<String, String> esProperties ;
  
  private Node server ;
  
  public Logger getLogger() { return this.logger; }
  
  public void start() throws Exception {
    Map<String, String> properties = new HashMap<String, String>() ;
    properties.put("cluster.name", "neverwinterdp");
    properties.put("path.data",    "./build/elasticsearch");
    logger.info(
        "ElasticSearch default properties:\n" + 
        JSONSerializer.INSTANCE.toString(properties)
    );
    if(esProperties != null) {
      properties.putAll(esProperties);
      logger.info(
          "ElasticSearch overrided properties:\n" + 
          JSONSerializer.INSTANCE.toString(properties)
      );
    }
    
    NodeBuilder nb = nodeBuilder();
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      nb.getSettings().put(entry.getKey(), entry.getValue());
    }
    server = nb.node();
  }

  public void stop() {
    server.close();
  }
}