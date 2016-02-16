package com.neverwinterdp.wa.event.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class SiteVisit {
  private String       site ;
  private List<String> pages ;

  public SiteVisit(String site, int numOfPages) {
    this.site  = site;
    this.pages = new ArrayList<>();
    for(int i = 1; i <= numOfPages; i++) {
      pages.add(site + "/page-" + i);
    }
    long seed = System.nanoTime();
    Collections.shuffle(pages, new Random(seed));
  }
  
  public String getSite() { return this.site; }
  
  public List<String> getPages() { return this.pages; }
}
