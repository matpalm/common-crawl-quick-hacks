package com.matpalm;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExtractUrlStatusCodes extends Configured implements Tool {
  
  public static void main(String args[]) throws Exception {
    ToolRunner.run(new ExtractUrlStatusCodes(), args);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length!=2) {
      throw new RuntimeException("usage: "+getClass().getName()+" <input> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName(getClass().getName());
    
    conf.setMapOutputKeyClass(Text.class);   
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
        
    conf.setMapperClass(ExtractUrlStatusCodesMapper.class);    
    conf.setNumReduceTasks(0); // map only    
        
    conf.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    if (optSet(conf, "compressOutput")) {
      System.err.println("compressing output with sequence files");;
      conf.set("mapred.output.compress", "true");
      conf.set("mapred.output.compression.type", "BLOCK");
      conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
      conf.setOutputFormat(SequenceFileOutputFormat.class);
    }

    JobClient.runJob(conf);
    
    return 0;
  }
  
  private static boolean optSet(JobConf conf, String opt) {
    return conf.get(opt)!=null && Boolean.parseBoolean(conf.get(opt));
  }
  
  public static class ExtractUrlStatusCodesMapper extends MapReduceBase implements Mapper<Text,Text,Text,Text> {

    private JsonParser parser;
    
    public void configure(JobConf job) {
      super.configure(job);
      parser = new JsonParser();
    }
    
    public void map(Text url, Text metadataText, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
      try {
        JsonObject metaData = parser.parse(metadataText.toString()).getAsJsonObject();        
        String response = metaData.getAsJsonObject("http_headers").get("response").getAsString();
        collector.collect(url, new Text(response));
      }
      catch (Exception e) {
        reporter.getCounter("error", e.getClass().getName()).increment(1);
      }
    }
    
  }
  
}
