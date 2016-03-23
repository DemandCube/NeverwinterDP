package com.neverwinterdp.analytics.web.stat;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class VisitorStat {
  static public String     SEED_ID = UUID.randomUUID().toString();
  static public AtomicLong ID_TRACKER = new AtomicLong();
  
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date            timestamp;
  
  private String          host;
  private String          visitorId;
  private int             hitCount;
  private HashSet<String> tags = new HashSet<String>();

  public VisitorStat() { }
  
  public VisitorStat(Date timestamp, String host, String visitorId) {
    this.timestamp  = timestamp;
    this.host       = host;
    this.visitorId  = visitorId;
  }
  
  public String uniqueId() { return SEED_ID + "-" + ID_TRACKER.incrementAndGet(); }
  
  public String hostVistorId() { return host + ":" + visitorId; }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getVisitorId() { return visitorId; }
  public void   setVisitorId(String visitorId) { this.visitorId = visitorId; }

  public int getHitCount() { return hitCount; }
  public void setHitCount(int hitCount) { this.hitCount = hitCount; }

  public String[] getTags() { return tags.toArray(new String[tags.size()]); }

  public void setTags(String[] tags) {
    for(String sel : tags) this.tags.add(sel);
  }
  
  public void addTag(String tag) { tags.add(tag); }
  
  public void log(long periodTimestamp, String host, String visitorId, WebEvent webEvent) {
    hitCount++;
    if(host.startsWith("www.website-")) {
      addTag("source:generator");
    } else {
      addTag("source:user");
    }
  }
  
  static public String getFormattedText(List<VisitorStat> holder) {
    TabularFormater formatter = new TabularFormater(
      "Host", "Visitor Id", "Timestamp", "Hit Count"
    ) ;
    formatter.setTitle("Statistic");
    for(int i = 0; i < holder.size(); i++) {
      VisitorStat wpStat = holder.get(i);
      String timestamp = DateUtil.asCompactDateTime(wpStat.getTimestamp());
      formatter.addRow(wpStat.getHost(), wpStat.getVisitorId(), timestamp, wpStat.getHitCount());
    }
    return formatter.getFormattedText();
  }
}