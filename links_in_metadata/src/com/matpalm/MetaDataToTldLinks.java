package com.matpalm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MetaDataToTldLinks {
  
  private static ParseResult NO_LINKS = new ParseResult(new HashSet<String>(), 0);
  private JsonParser parser;
  
  public static void main(String[] s) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(s[0]));
    MetaDataToTldLinks metaDataToTldLinks = new MetaDataToTldLinks();
    while (reader.ready()) {
      String[] fields = reader.readLine().split("\t");
      ParseResult outboundLinks = metaDataToTldLinks.outboundLinks(fields[1]);
      System.out.println(tldOf(fields[0]) + " " + outboundLinks.links);      
    }
  }
  
  public MetaDataToTldLinks() {
    this.parser = new JsonParser();
  }
  
  public ParseResult outboundLinks(String jsonMetaData) {
    JsonObject metaData = parser.parse(jsonMetaData.toString()).getAsJsonObject();
    
    if (!"SUCCESS".equals(metaData.get("disposition").getAsString()))
      return NO_LINKS;
    
    JsonElement content = metaData.get("content");
    if (content == null)
      return NO_LINKS;
    
    JsonArray links = content.getAsJsonObject().getAsJsonArray("links");
    if (links == null)
      return NO_LINKS;
    
    Set<String> outboundLinks = new HashSet<String>();
    int numNull = 0;
    for (JsonElement linke : links) {
      JsonObject link = linke.getAsJsonObject();
      if ("a".equals(link.get("type").getAsString())) { // anchor        
        String tld = tldOf(link.get("href").getAsString());
        if (tld == null)
          ++numNull;
        else
          outboundLinks.add(tld);
      }
    }
    return new ParseResult(outboundLinks, numNull);
    
  }
  
  public static String tldOf(String url) {
    try {
      String tld = new URI(url).getHost();
      if (tld.startsWith("www."))
        tld = tld.substring(4);
      tld = tld.trim();
      return tld.length()==0 ? null : tld;
    }
    catch (URISyntaxException e) {
      return null;
    }
  }
   
  public static class ParseResult {
    public final Set<String> links;
    public final int numNull;
    public ParseResult(Set<String> links, int numNull) {
      this.links = links;
      this.numNull = numNull;
    }
  }
  
}
