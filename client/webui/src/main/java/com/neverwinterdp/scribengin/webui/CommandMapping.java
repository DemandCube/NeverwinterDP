package com.neverwinterdp.scribengin.webui;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.util.JSONSerializer;

public class CommandMapping {
  private Map<String, Class<? extends Command>> holder = new HashMap<>();

  public void add(String name, Class<? extends Command> clazz) {
    holder.put(name, clazz);
  }
  
  public <T extends Command> T getInstance(String name, byte[] data) throws Exception {
    Class<? extends Command> type =  holder.get(name);
    T instance = (T)JSONSerializer.INSTANCE.fromBytes(data, type);
    return instance;
  }
  
  public <T extends Command> T getInstance(String name, Map<String, List<String>> params) throws Exception {
    List<String> argsHolder = new ArrayList<>();
    for(Map.Entry<String, List<String>> entry : params.entrySet()) {
      argsHolder.add("--" + entry.getKey());
      for(String val : entry.getValue()) {
        argsHolder.add(val);
      }
    }
    String[] args = new String[argsHolder.size()];
    argsHolder.toArray(args);
    Class<? extends Command> type =  holder.get(name);
    T instance = (T) type.newInstance();
    new JCommander(instance, args);
    return instance;
  }
}
