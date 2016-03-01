package com.neverwinterdp.analytics.web.stat;

import java.util.Date;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.util.UrlParser;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class WebPageStat {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  
  private String          host;
  private String          pagePath;
  private int             hitCount;
  private HashSet<String> tags = new HashSet<String>();

  public WebPageStat() { }
  
  public WebPageStat(Date timestamp, UrlParser urlParser) {
    this.timestamp = timestamp;
    this.host      = urlParser.getHost();
    this.pagePath  = urlParser.getPath();
  }
  
  public String uniqueId() {
    return host + pagePath + "#" + DateUtil.asCompactDateTime(timestamp);
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public String getPagePath() { return pagePath; }
  public void setPagePath(String pagePath) { this.pagePath = pagePath; }

  public int getHitCount() { return hitCount; }
  public void setHitCount(int hitCount) { this.hitCount = hitCount; }

  public String[] getTags() { return tags.toArray(new String[tags.size()]); }

  public void setTags(String[] tags) {
    for(String sel : tags) this.tags.add(sel);
  }

  public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
    hitCount++;
    if(urlParser.getHost().startsWith("www.website-")) {
      addTag("source:generator");
    } else {
      addTag("source:user");
    }
  }
  
  public void addTag(String tag) {
    tags.add(tag);
  }
  
  static public String getFormattedText(List<WebPageStat> holder) {
    TabularFormater formatter = new TabularFormater(
      "Host", "Page Path", "Timestamp", "Hit Count"
    ) ;
    formatter.setTitle("Web Page Statistic");
    for(int i = 0; i < holder.size(); i++) {
      WebPageStat wpStat = holder.get(i);
      String timestamp = DateUtil.asCompactDateTime(wpStat.getTimestamp());
      formatter.addRow(wpStat.getHost(), wpStat.getPagePath(), timestamp, wpStat.getHitCount());
    }
    return formatter.getFormattedText();
  }
}