package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.neverwinterdp.os.RuntimeEnv;

abstract public class ServiceModule extends AbstractModule {
  private Map<String, String> properties = new HashMap<String, String>();
  private Map<String, String> overridedProperties ;

  public void init(Map<String, String> overridedProperties, RuntimeEnv env) {
    this.overridedProperties = overridedProperties ;
    properties.put("module.data.drop", "false") ;
    properties.put("module.data.dir", env.getDataDir()) ;
  }
  
  final protected void configure() {
    configure(properties) ;
    bindProperties() ;
  }
  
  abstract protected void configure(Map<String, String> properties) ;
  
  protected <T> void bindInstance(String serviceId, T instance) {
    Key<T> key = (Key<T>)Key.get(instance.getClass(), Names.named(serviceId)) ;
    bind(key).toInstance(instance); 
  }
  
  protected <T> void bindType(String serviceId, Class<T> type) {
    Key<T> key = Key.get(type, Names.named(serviceId)) ;
    bind(key).to(type).asEagerSingleton(); ;
  }
  
  void bindProperties() {
    if(overridedProperties != null) {
      for(Map.Entry<String, String> sel : overridedProperties.entrySet()) {
        properties.put(sel.getKey(), sel.getValue()) ;
      }
    }
    PropertiesSet propertiesSet = new PropertiesSet() ;
    Iterator<Map.Entry<String, String>> i = properties.entrySet().iterator() ;
    while(i.hasNext()) {
      Map.Entry<String, String> entry = i.next() ;
      propertiesSet.add(entry.getKey(), entry.getValue());
    }
    for(Map.Entry<String, Map<String, String>> entry : propertiesSet.entrySet()) {
      String mapName = entry.getKey() ;
      if(mapName.equals("")) {
        Names.bindProperties(binder(), entry.getValue()) ;
      } else {
        String name = mapName + "Properties" ;
        Map<String, String> map = entry.getValue() ;
        bind(new TypeLiteral<Map<String,String>>(){}).annotatedWith(Names.named(name)).toInstance(map) ;
      }
    }
  }
  
  static public class PropertiesSet extends HashMap<String, Map<String, String>> {
    public void add(String key, String value) {
      put("", key, value) ;
      String mapName = "" ;
      int separatorPos = key.indexOf(":") ;
      if(separatorPos > 0) {
        mapName = key.substring(0, separatorPos) ;
        key = key.substring(separatorPos + 1);
        put(mapName, key, value) ;
      }
    }
    
    void put(String mapName, String key, String value) {
      Map<String, String> map = get(mapName) ;
      if(map == null) {
        map = new HashMap<String, String>() ;
        put(mapName, map) ;
      }
      map.put(key, value) ;
    }
  }
}