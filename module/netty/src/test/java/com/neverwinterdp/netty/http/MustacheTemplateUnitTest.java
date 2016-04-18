package com.neverwinterdp.netty.http;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.neverwinterdp.util.io.IOUtil;

public class MustacheTemplateUnitTest {
  @Test
  public void testPOJO() throws Exception {
    String template = IOUtil.getFileContentAsString("src/test/resources/mustache/hello.mtpl") ;
    MustacheFactory mf = new DefaultMustacheFactory();
    Mustache mustache = mf.compile(new StringReader(template), "hello");
    mustache.execute(new PrintWriter(System.out), new MustacheTemplateUnitTest()).flush();
  }
  
  
  @Test
  public  void testMap() throws Exception {
    HashMap<String, Object> scopes = new HashMap<String, Object>();
    scopes.put("name", "Mustache");
    scopes.put("feature", new Feature("Perfect!"));

    Writer writer = new OutputStreamWriter(System.out);
    MustacheFactory mf = new DefaultMustacheFactory();
    Mustache mustache = mf.compile(new StringReader("{{name}}, {{feature.description}}!\n\n"), "example");
    mustache.execute(writer, scopes);
    writer.flush();
  }
  
  List<Item> items() {
    return Arrays.asList(
      new Item("Item 1", "$19.99", Arrays.asList(new Feature("New!"), new Feature("Awesome!"))),
      new Item("Item 2", "$29.99", Arrays.asList(new Feature("Old."), new Feature("Ugly.")))
    );
  }

  static class Item {
    Item(String name, String price, List<Feature> features) {
      this.name = name;
      this.price = price;
      this.features = features;
    }

    String name, price;
    List<Feature> features;
  }

  static class Feature {
    Feature(String description) {
      this.description = description;
    }

    String description;
  }
}
