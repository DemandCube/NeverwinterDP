package com.neverwinterdp.util.log;

import java.io.Serializable;
import java.util.Date;

import org.apache.log4j.spi.LoggingEvent;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.ExceptionUtil;

public class Log4jRecord implements Serializable {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date     timestamp;
  private String   threadName;
  private String   loggerName;
  private String   level;
  private String   message;
  private String   stacktrace;
  private String[] tag;

  public Log4jRecord() {
  }

  public Log4jRecord(LoggingEvent event) {
    this.timestamp = new Date(event.getTimeStamp());
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
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

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
