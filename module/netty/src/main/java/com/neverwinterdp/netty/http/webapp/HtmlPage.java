package com.neverwinterdp.netty.http.webapp;

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.util.Map;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.neverwinterdp.util.io.IOUtil;

abstract public class HtmlPage {
  protected MustacheFactory mFactory ;
  private Mustache mustacheTmpl ;
  
  public HtmlPage(String name, String templateRes) throws IOException {
    mFactory = new DefaultMustacheFactory();
    String template = IOUtil.loadResAsString(templateRes) ;
    mustacheTmpl = mFactory.compile(new StringReader(template), name);
  }
  
  abstract public  void render(Writer writer, Map<String, Object> scopes) throws Exception ;
  
  protected  void renderHtmlPage(Writer writer, Map<String, Object> scopes) throws Exception {
    mustacheTmpl.execute(writer, scopes);
    writer.flush() ;
  }
}