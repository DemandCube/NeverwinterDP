package com.neverwinterdp.analytics.odyssey.generator;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.analytics.odyssey.ActionEvent;

public class ActionEventGenerator {
  private String         seedId = UUID.randomUUID().toString();
  private AtomicLong  idTracker = new AtomicLong();
  
  public ActionEvent nextEvent() {
    ActionEvent event = new ActionEvent();
    event.setSource("generator");
    event.setEventId("odyssey-action-event-" + seedId + "-" + idTracker.incrementAndGet());
    event.setTimestamp(new Date());
    return event;
  }
}