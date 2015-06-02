package com.neverwinterdp.util.log;

import java.io.Serializable;
import java.util.Date;

import org.apache.log4j.spi.LoggingEvent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.ExceptionUtil;

public class Log4jRecord implements Serializable {
  private long     timestamp;
  private String   threadName;
  private String   loggerName;
  private String   level;
  private String   message;
  private String   stacktrace;
  private String[] tag;

  public Log4jRecord() {
  }

  public Log4jRecord(LoggingEvent event) {
    this.timestamp = event.getTimeStamp();
    this.threadName = event.getThreadName() ;
    this.loggerName = event.getLoggerName();
    this.level = event.getLevel().toString();
    this.message = event.getRenderedMessage();
    if(event.getThrowableInformation() != null) {
      stacktrace = ExceptionUtil.getStackTrace(event.getThrowableInformation().getThrowable());
    }
  }

  @JsonIgnore
  public String getId() {
    return this.loggerName + "-"  + this.timestamp + "-" + message.hashCode() ;
  }
  
  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  public Date getCreatedTime() { return new Date(timestamp) ; }
  public void setCreatedTime(Date timestamp) {  }
  
  public String getThreadName() { return threadName; }
  public void setThreadName(String threadName) { this.threadName = threadName; }

  public String getLoggerName() { return loggerName; }
  public void setLoggerName(String loggerName) { this.loggerName = loggerName; }

  public String getLevel() { return level; }
  public void setLevel(String level) { this.level = level; }

  public String getMessage() { return message; }
  public void setMessage(String message) { this.message = message; }

  public String getStacktrace() { return stacktrace; }
  public void setStacktrace(String stacktrace) { this.stacktrace = stacktrace; }

  public String[] getTag() { return tag; }
  public void setTag(String[] tag) { this.tag = tag; }
}
