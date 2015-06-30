package com.neverwinterdp.module;

import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;

abstract public class ServiceModule extends AbstractModule {
  private Map<String, String> properties ;
  
  public void setProperties(Map<String, String> props) {
    properties = props ;
  }
  
  @Override
  final protected void configure() {
    configure(properties);
  }
  
  abstract protected void configure(Map<String, String> props) ;
  
  protected <T> void bindInstance(String id, T instance) {
    Key<T> key = (Key<T>)Key.get(instance.getClass(), Names.named(id)) ;
    bind(key).toInstance(instance); 
  }
  
  protected <T> void bindType(String id, Class<T> type) {
    Key<T> key = Key.get(type, Names.named(id)) ;
    bind(key).to(type).asEagerSingleton(); ;
  }
  
  public <T> void bindType(Class<T> type, String impl) throws ClassNotFoundException {
    Class<?> clazz = (Class<?>)Class.forName(impl);
    bind(type).to((Class)clazz);
  }
  
  public <T> void bindType(Class<T> type, Class<? extends T> clazz) {
    bind(type).to(clazz);
  }
  
  public <T> void bindInstance(Class<T> type, T instance) {
    bind(type).toInstance(instance);
  }
}