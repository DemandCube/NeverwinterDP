package com.neverwinterdp.wa.stat;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.util.UrlParser;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.wa.event.WebEvent;

public class WebPageStat {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  
  private String host;
  private String pagePath;
  
  private int    hitCount;
  
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

  public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
    hitCount++;
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