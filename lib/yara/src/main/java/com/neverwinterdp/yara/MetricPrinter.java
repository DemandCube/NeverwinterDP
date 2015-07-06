package com.neverwinterdp.yara;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.yara.snapshot.MeterSnapshot;
import com.neverwinterdp.yara.snapshot.TimerSnapshot;

public class MetricPrinter {
  private Appendable out = System.out ;
  
  public MetricPrinter() { }
  
  public MetricPrinter(Appendable out) { 
    this.out = out ;
  }
  
  public void print(MetricRegistry registry) throws IOException {
    printCounters(registry.getCounters()) ;
    printMeters(registry.getMeters());
    printTimers(registry.getTimers()) ;
  }
  
  public void printTimers(Map<String, Timer> timers) throws IOException {
    TimerPrinter tprinter = new TimerPrinter(out) ;
    String[] keys = StringUtil.toSortedArray(timers.keySet()) ;
    for(String key : keys) {
      tprinter.print(key, timers.get(key));
    }
    tprinter.flush(); 
  }
  
  public void printMeters(Map<String, Meter> meters) throws IOException {
    MeterPrinter tprinter = new MeterPrinter(out) ;
    String[] keys = StringUtil.toSortedArray(meters.keySet()) ;
    for(String key : keys) {
      tprinter.print(key, meters.get(key));
    }
    tprinter.flush(); 
  }
  
  public void printCounters(Map<String, Counter> counters) throws IOException {
    CounterPrinter cPrinter = new CounterPrinter(out) ;
    String[] keys = StringUtil.toSortedArray(counters.keySet()) ;
    for(String key : keys) {
      cPrinter.print(key, counters.get(key));
    }
    cPrinter.flush(); 
  }
  
  static public class TimerPrinter {
    private Appendable out = System.out;
    protected DecimalFormat dFormater = new DecimalFormat("#");
    protected TabularFormater  tformater ;
    protected TimeUnit timeUnit = TimeUnit.NANOSECONDS ;
    
    public TimerPrinter() { 
      tformater = new TabularFormater(
          "Name", "Count",
          "Min", "Max", "Mean", "Std Dev",
          "75%", "90%", "95%", "99%", "99.999",
          "1 Min", "5 Min", "15 Min", "M Rate"
      ) ;
      tformater.setTitle("Timer") ;
    }
    
    public TimerPrinter(Appendable out) { 
      this() ;
      this.out = out ; 
    }
    
    public void print(String name, Timer timer) {
      Histogram histogram = timer.getHistogram() ;
      tformater.addRow(
        name, 
        timer.getCount(),
        
        dFormater.format(histogram.getMin()), 
        dFormater.format(histogram.getMax()), 
        dFormater.format(histogram.getMean()), 
        dFormater.format(histogram.getStdDev()), 
        
        dFormater.format(histogram.getQuantile(0.75)),
        dFormater.format(histogram.getQuantile(0.90)),
        dFormater.format(histogram.getQuantile(0.95)),
        dFormater.format(histogram.getQuantile(0.99)),
        dFormater.format(histogram.getQuantile(0.99999)),
        
        dFormater.format(timer.getOneMinuteRate()), 
        dFormater.format(timer.getFiveMinuteRate()), 
        dFormater.format(timer.getFifteenMinuteRate()),
        dFormater.format(timer.getMeanRate())
      );
    }
    
    public void print(String name, TimerSnapshot timer) {
      tformater.addRow(
        name, 
        timer.getCount(),
        
        dFormater.format(timer.getMin()), 
        dFormater.format(timer.getMax()), 
        dFormater.format(timer.getMean()), 
        dFormater.format(timer.getStddev()), 
        
        dFormater.format(timer.getP75()),
        dFormater.format(timer.getP90()),
        dFormater.format(timer.getP99()),
        dFormater.format(timer.getP999()),
        dFormater.format(timer.getP999()),
        
        dFormater.format(timer.getM1Rate()), 
        dFormater.format(timer.getM5Rate()), 
        dFormater.format(timer.getM15Rate()),
        dFormater.format(timer.getMeanRate())
      );
    }
    
    public void flush() throws IOException {
      out.append("\n") ;
      out.append(tformater.getFormatText());
      out.append("\n") ;
    }
  }
  
  static public class MeterPrinter {
    private Appendable out = System.out;
    protected DecimalFormat dFormater = new DecimalFormat("#");
    protected TabularFormater  tformater ;
    protected TimeUnit timeUnit = TimeUnit.NANOSECONDS ;
    
    public MeterPrinter() { 
      tformater = new TabularFormater(
        "Name", "Count", "1 Min", "5 Min", "15 Min", "M Rate"
      ) ;
      tformater.setTitle("Meter") ;
    }
    
    public MeterPrinter(Appendable out) { 
      this() ;
      this.out = out ; 
    }
    
    public void print(String name, Meter meter) {
      String rate = " " + meter.getUnit() +"/s" ;
      tformater.addRow(
        name, 
        meter.getCount(),
        dFormater.format(meter.getOneMinuteRate()) + rate, 
        dFormater.format(meter.getFiveMinuteRate()) + rate, 
        dFormater.format(meter.getFifteenMinuteRate()) + rate,
        dFormater.format(meter.getMeanRate()) + rate
      );
    }
    
    public void print(String name, MeterSnapshot meter) {
      String rate = " " + meter.getUnit() +"/s" ;
      tformater.addRow(
        name, 
        meter.getCount(),
        dFormater.format(meter.getM1Rate())  + rate, 
        dFormater.format(meter.getM5Rate())  + rate, 
        dFormater.format(meter.getM15Rate())  + rate,
        dFormater.format(meter.getMeanRate()) + rate
      );
    }
    
    public void flush() throws IOException {
      out.append("\n") ;
      out.append(tformater.getFormatText());
      out.append("\n") ;
    }
  }
  
  static public class CounterPrinter {
    private Appendable out = System.out;
    protected TabularFormater  tformater ;
    
    public CounterPrinter() { 
      tformater = new TabularFormater("Name", "Count") ;
      tformater.setTitle("Counter") ;
    }
    
    public CounterPrinter(Appendable out) { 
      this() ;
      this.out = out ; 
    }
    
    public void print(String name, Counter counter) {
      tformater.addRow(name, counter.getCount());
    }
    
    public void print(String name, long count) {
      tformater.addRow(name, count);
    }

    public void flush() throws IOException {
      out.append("\n") ;
      out.append(tformater.getFormatText());
      out.append("\n") ;
    }
  }
}
