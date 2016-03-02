package com.neverwinterdp.analytics.web.stat;

import java.util.Date;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.analytics.web.WebEvent;

public class SpentTimeStat {
  static public String     SEED_ID = UUID.randomUUID().toString();
  static public AtomicLong ID_TRACKER = new AtomicLong();
  
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date            timestamp;
  
  private String          host;
  private int             about5SecCount;
  private int             about10SecCount;
  private int             about30SecCount;
  private int             about1MinCount ;
  private int             about5MinCount ;
  private int             about10MinCount ;
  
  private HashSet<String> tags = new HashSet<String>();

  public SpentTimeStat() { }
  
  public SpentTimeStat(Date timestamp, String host) {
    this.timestamp  = timestamp;
    this.host       = host;
  }
  
  public String uniqueId() { return SEED_ID + "-" +ID_TRACKER.incrementAndGet(); }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }
  
  public int getAbout5SecCount() { return about5SecCount; }
  public void setAbout5SecCount(int about5SecCount) { this.about5SecCount = about5SecCount; }

  public int getAbout10SecCount() { return about10SecCount; }
  public void setAbout10SecCount(int about10SecCount) { this.about10SecCount = about10SecCount; }

  public int getAbout30SecCount() { return about30SecCount; }
  public void setAbout30SecCount(int about30SecCount) { this.about30SecCount = about30SecCount; }

  public int getAbout1MinCount() { return about1MinCount; }
  public void setAbout1MinCount(int about1MinCount) { this.about1MinCount = about1MinCount; }

  public int getAbout5MinCount() { return about5MinCount; }
  public void setAbout5MinCount(int about5MinCount) { this.about5MinCount = about5MinCount; }

  public int getAbout10MinCount() { return about10MinCount; }
  public void setAbout10MinCount(int about10MinCount) { this.about10MinCount = about10MinCount; }

  public String[] getTags() { return tags.toArray(new String[tags.size()]); }

  public void setTags(String[] tags) {
    for(String sel : tags) this.tags.add(sel);
  }
  
  public void addTag(String tag) { tags.add(tag); }
  
  public void log(long periodTimestamp, String host, WebEvent webEvent) {
    long spentTime = webEvent.getClientInfo().user.spentTime;
    if(spentTime < (5 * 1000))  about5SecCount++;
    else if(spentTime < (10 * 1000)) about10SecCount++;
    else if(spentTime < (30 * 1000)) about30SecCount++;
    else if(spentTime < (60 * 1000)) about1MinCount++;
    else if(spentTime < (5 * 60 * 1000)) about5MinCount++;
    else  about10MinCount++;
  }
}