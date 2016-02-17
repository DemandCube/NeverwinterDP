package com.neverwinterdp.wa.event.generator;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.wa.event.BrowserInfo;
import com.neverwinterdp.wa.event.WebEvent;

public class WebEventGenerator {
  private AtomicInteger idTracker = new AtomicInteger();
  
  public WebEvent next(BrowserInfo bInfo, String name, String method, String url) {
    WebEvent event = new WebEvent();
    event.setEventId(Integer.toString(idTracker.incrementAndGet()));
    event.setTimestamp(System.currentTimeMillis());
    event.setName(name);
    event.setMethod(method);
    event.setUrl(url);
    event.setBrowserInfo(bInfo);
    return event;
  }
}
