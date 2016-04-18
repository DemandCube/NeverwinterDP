package com.neverwinterdp.es.sysinfo;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
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
    nb.getSettings().put("node.name",          "127.0.0.1");
    nb.getSettings().put("transport.tcp.port", "9300");

    node = nb.node();
  }
  
  @After
  public void after() {
    node.close();
  }
  
  @Test
  public void testService() throws Exception {
    SysInfoLoggerService service1 = new SysInfoLoggerService();
    service1.onInit(new RuntimeEnv("localhost", "vm-1", WORKING_DIR));
    service1.setLogPeriod(3000);
    
    SysInfoLoggerService service2 = new SysInfoLoggerService();
    service2.onInit(new RuntimeEnv("localhost", "vm-2", WORKING_DIR));
    service2.setLogPeriod(3000);
    
    Thread.sleep(15000);
    service1.onDestroy();
    service2.onDestroy();
    
    ESClient esclient = new ESClient(new String[] { "127.0.0.1:9300" });
    ESObjectClient<SysInfo> esObjecClient = new ESObjectClient<SysInfo>(esclient, "neverwinterdp-sys-info", SysInfo.class) ;
    System.out.println("SysMetric records = " + esObjecClient.count(termQuery("host", "vm-1")));
    System.out.println("SysMetric Heap Memory = " + esObjecClient.count(termQuery("metric.mem.name", "Heap")));
    
    AbstractAggregationBuilder[] aggregationBuilders = { 
      AggregationBuilders.terms("by_vmName").field("host"),
      AggregationBuilders.count("count_by_vmName").field("host"),
      AggregationBuilders.
        dateRange("by_timestamp").field("timestamp").format("dd/MM/yyyy HH:mm:ss").
        addRange("8:00PM  - 10:00PM", "04/04/2016 10:00:00", "04/04/2016 22:00:00")
    };
    SearchResponse searchResponse = esObjecClient.search(null, aggregationBuilders, true, 0, 3);
    SearchHits hits = searchResponse.getHits();
    System.out.println("Total  Hits = " + hits.getTotalHits());
    System.out.println("Return Hits = " + hits.getHits().length);
    Aggregations aggregations = searchResponse.getAggregations() ;
    dump(aggregations, "");
    
    System.out.println(esObjecClient.getQueryExecutor().matchAll().setAggregations(aggregationBuilders).executeAndReturnAsJson());
  }
  
  private void dump(Aggregations aggregations, String indent) {
    for(Aggregation sel : aggregations.asList()) {
      System.out.println(indent + "aggregation = " + sel.getName() + ", type = " + sel.getClass());
      if(sel instanceof StringTerms) {
        StringTerms stringTerms = (StringTerms) sel;
        List<Terms.Bucket> buckets = stringTerms.getBuckets() ;
        for(Terms.Bucket selBucket : buckets) {
          System.out.println(indent + "  key = " + selBucket.getKey()) ;
          System.out.println(indent + "     doc count = " + selBucket.getDocCount());
        }
      } else if(sel instanceof InternalValueCount) {
        InternalValueCount count = (InternalValueCount) sel;
        System.out.println(indent + "  count = " + count.getValue()) ;
      } else if(sel instanceof InternalDateRange) {
        InternalDateRange dateRange = (InternalDateRange)sel;
        System.out.println(indent + "  dateRange = " + dateRange.getName()) ;
        Collection<InternalDateRange.Bucket> buckets = dateRange.getBuckets();
        for(InternalDateRange.Bucket bucket : buckets) {
          System.out.println(indent + "    key = " + bucket.getKey()) ;
          System.out.println(indent + "    from = " + bucket.getFromAsString()) ;
          System.out.println(indent + "    to = " + bucket.getToAsString()) ;
          System.out.println(indent + "    doc count = " + bucket.getDocCount()) ;
        }
      }
    }
  }
}
