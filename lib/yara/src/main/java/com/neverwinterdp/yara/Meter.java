package com.neverwinterdp.yara;

import java.io.Serializable;
import java.util.Collection;

public class Meter implements Serializable {
  transient private MetricPlugin metricPlugin ;
  private String name ;
  private String unit = "call";
  
  private EWMAMeter ewmaMeter = new EWMAMeter() ;

  public Meter() {} 
  
  public Meter(String name, String unit) {
    this.name = name ;
    this.unit = unit;
  }
  
  public Meter(String name, EWMAMeter meter, String unit) {
    this.name = name ;
    this.ewmaMeter = meter;
    this.unit = unit;
  }
  
  public String getName() { return this.name ; }
  
  public String getUnit() { return this.unit; }
  
  public Meter setUnit(String unit) {
    this.unit = unit;
    return this;
  }
  
  public long getCount() { return ewmaMeter.getCount() ; }
  
  public void setMetricPlugin(MetricPlugin plugin) {
    this.metricPlugin = plugin ;
  }
  
  public long mark(long n) {
    mark(Clock.defaultClock().getTick(), n) ;
    return n;
  }
  
  public long mark(long timestampTick, long n) {
    if(metricPlugin != null) {
      metricPlugin.onCounterAdd(name, timestampTick, n);
    }
    return ewmaMeter.mark(timestampTick, n);
  }

  public double getOneMinuteRate() { return ewmaMeter.getOneMinuteRate(); }

  public double getFiveMinuteRate() { return ewmaMeter.getFiveMinuteRate(); }
  
  public double getFifteenMinuteRate() { return ewmaMeter.getFifteenMinuteRate(); }

  public double getMeanRate() { return ewmaMeter.getMeanRate(); }

  
  static public Meter unionOf(Meter meter1, Meter meter2) {
    String name = meter1.getName() ;
    if(name == null || !name.equals(meter2.getName())) {
      throw new RuntimeException("timer name is null or not equals") ;
    }
    EWMAMeter eWMAMeter = EWMAMeter.unionOf(meter1.ewmaMeter, meter2.ewmaMeter) ;
    return new Meter(name, eWMAMeter, meter1.unit) ;
  }
  
  static public Meter combine(Meter ... meter) {
    if(meter.length == 0) return new Meter() ;
    else if(meter.length == 1) return meter[0] ;
    Meter combine = Meter.unionOf(meter[0], meter[1]) ;
    for(int i = 2; i < meter.length; i++) {
      combine = Meter.unionOf(combine, meter[i]) ;
    }
    return combine ;
  }
  
  static public Meter combine(Collection<Meter>  counters) {
    Meter[] array = new Meter[counters.size()] ;
    counters.toArray(array) ;
    return combine(array) ;
  }
}
