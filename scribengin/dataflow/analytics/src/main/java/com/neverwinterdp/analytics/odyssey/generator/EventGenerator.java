package com.neverwinterdp.analytics.odyssey.generator;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.analytics.odyssey.Event;

public class EventGenerator {
  private AtomicLong idTracker = new AtomicLong();
  
  public Event nextEvent() {
    Event event = new Event();
    event.setEventId("odyssey-event-" + idTracker.incrementAndGet());
    event.setTimestamp(new Date());
    event.setJson("{ }");
    return event;
  }
}
