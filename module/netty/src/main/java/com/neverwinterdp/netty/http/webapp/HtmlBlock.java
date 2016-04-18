package com.neverwinterdp.netty.http.webapp;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.neverwinterdp.util.io.IOUtil;

public class HtmlBlock {
  private String   name ;
  private String   templateResource ;
  private Mustache mustache ;
  
  public HtmlBlock(String name, String templateRes, MustacheFactory mf) throws IOException {
    this.name = name ;
    this.templateResource = templateRes ;
    String template = IOUtil.loadResAsString(templateRes) ;
    mustache = mf.compile(new StringReader(template), name);
  }
  
  public String getName() { return this.name ; }
  
  public String getTemplateResource() { return this.templateResource ; }
  
  public  String toHtml(Map<String, Object> scopes) throws Exception {
    StringWriter writer = new StringWriter() ;
    mustache.execute(writer, scopes);
    writer.flush();
    return writer.toString() ;
  }
}