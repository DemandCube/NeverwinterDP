package com.neverwinterdp.es.sysinfo;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.text.DateUtil;

public class SysInfo implements Serializable {
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
  private Date                timestamp;
  private String              host;
  private Map<String, Object> metric = new HashMap<String, Object>();

  public String uniqueId() { 
    return "host=" + host + ",timestamp=" + DateUtil.asCompactDateTimeId(timestamp); 
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }
  
  public Map<String, Object> getMetric() { return metric; }
  public void setMetric(Map<String, Object> metric) { this.metric = metric; }

  public void add(String name, Object metricObj) { metric.put(name, metricObj); }
}
