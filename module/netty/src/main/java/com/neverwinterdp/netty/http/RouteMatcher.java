package com.neverwinterdp.netty.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class RouteMatcher<T> {
  private List<PatternBinding<T>> bindings = new ArrayList<PatternBinding<T>>() ;
  private T defaultHandler ; 
  
  public T getDefaultHandler() { return this.defaultHandler ; }
  public void setDefaultHandler(T handler) {
    this.defaultHandler = handler ;
  }
  
  public T findHandler(String path) {
    for (PatternBinding<T> binding: bindings) {
      Matcher m = binding.pattern.matcher(path);
      if (m.matches()) {
        Map<String, String> params = new HashMap<>(m.groupCount());
        if (binding.paramNames != null) {
          // Named params
          for (String param: binding.paramNames) {
            params.put(param, m.group(param));
          }
        } else {
          // Un-named params
          for (int i = 0; i < m.groupCount(); i++) {
            params.put("param" + i, m.group(i + 1));
          }
        }
        //TODO: add path params
        return binding.handler ;
      }
    }
    return defaultHandler ;
  }
  
  public void addPattern(String input, T handler) {
    // We need to search for any :<token name> tokens in the String and replace them with named capture groups
    Matcher m =  Pattern.compile(":([A-Za-z][A-Za-z0-9_]*)").matcher(input);
    StringBuffer sb = new StringBuffer();
    Set<String> groups = new HashSet<>();
    while (m.find()) {
      String group = m.group().substring(1);
      if (groups.contains(group)) {
        throw new IllegalArgumentException("Cannot use identifier " + group + " more than once in pattern string");
      }
      m.appendReplacement(sb, "(?<$1>[^\\/]+)");
      groups.add(group);
    }
    m.appendTail(sb);
    String regex = sb.toString();
    PatternBinding<T> binding = new PatternBinding<T>(Pattern.compile(regex), groups, handler);
    bindings.add(binding);
  }
  
  private static class PatternBinding<T> {
    final Pattern pattern;
    final T handler;
    final Set<String> paramNames;

    private PatternBinding(Pattern pattern, Set<String> paramNames, T handler) {
      this.pattern = pattern;
      this.paramNames = paramNames;
      this.handler = handler;
    }
  }
}
