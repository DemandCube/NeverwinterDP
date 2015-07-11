package com.neverwinterdp.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

abstract public class ListSelector<T> {
  private List<T> items = new ArrayList<T>() ;

  public int size() { return items.size(); }
  
  public T get(int idx) { return items.get(idx); }
  
  public void remove(int idx) { items.remove(idx); }
  
  public void remove(T item) { items.remove(item); }
  
  public void add(T item) {
    items.add(item);
  }
  
  abstract public T next() ;

  static public class RoundRobinListSelector<T> extends ListSelector<T> {
    private int currentIdx = 0;
    
    @Override
    public T next() {
      if(size() == 0) return null ;
      if(currentIdx == size()) {
        currentIdx = 0 ;
      }
      return get(currentIdx++);
    }
  }
  
  static public class RandomListSelector<T> extends ListSelector<T> {
    private Random random = new Random() ;
    
    @Override
    public T next() {
      if(size() == 0) return null ;
      return get(random.nextInt(size()));
    }
  }
}
