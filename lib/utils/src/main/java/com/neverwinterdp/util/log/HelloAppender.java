package com.neverwinterdp.util.log;

import java.util.Date;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class HelloAppender extends AppenderSkeleton {

  public void activateOptions() {
    System.out.println("Activate Hello Appender");
  }

  public boolean requiresLayout() { return false; }

  protected void append(LoggingEvent event) {
    System.out.println("time: " + new Date(event.getTimeStamp()));
    System.out.println("logger name: " + event.getLoggerName());
  }

  @Override
  public void close() {
  }
  
}
