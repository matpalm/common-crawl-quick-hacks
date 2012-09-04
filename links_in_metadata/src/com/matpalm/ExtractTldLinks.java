package com.matpalm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.matpalm.MetaDataToTldLinks.ParseResult;

/**
 * extracting links from the meta data data set.
 * 
 * either 
 *  hadoop jar foo.jar com.matpalm.ExtractTldLinks /path/to/inputs /path/to/output
 * or
 *  cat manifest_of_path_to_inputs | hadoop jar foo.jar com.matpalm.ExtractTldLinks /path/to/output 
 */
public class ExtractTldLinks extends Configured implements Tool {
  
  private static final IntWritable ONE = new IntWritable(1);
  
  /**
   * we always include a main as a simple way to be able to run directly in an ide without the hadoop stack 
   */
  public static void main(String args[]) throws Exception {    
    ToolRunner.run(new ExtractTldLinks(), args);
  }
  
  public int run(String[] args) throws Exception {

    JobConf conf = new JobConf(getConf(), getClass());

    if (args.length==1) {
      // assume inputs from stdin
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      while (in.ready())
        FileInputFormat.addInputPath(conf, new Path(in.readLine()));
      FileOutputFormat.setOutputPath(conf, new Path(args[0]));
    }
    else if (args.length==2) {
      FileInputFormat.addInputPath(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));      
    }
    else {
      throw new RuntimeException("usage: either "+getClass().getName()+" <input> <output> or"+
          " cat inputs | "+getClass().getName()+" output");
    }
    
    conf.setJobName(getClass().getName());   

    conf.setMapOutputKeyClass(Text.class);   
    conf.setMapOutputValueClass(IntWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Integer.class);
        
    conf.setMapperClass(ExtractUrlStatusCodesMapper.class);  
    conf.setCombinerClass(SumReducer.class);
    conf.setReducerClass(SumReducer.class);
        
    conf.setInputFormat(SequenceFileInputFormat.class);
    
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
  
  public static class ExtractUrlStatusCodesMapper extends MapReduceBase implements Mapper<Text,Text,Text,IntWritable> {

    // a parser for handling the json metadata
    private MetaDataToTldLinks metaDataToTldLinks;
    
    public void configure(JobConf job) {
      super.configure(job);
      metaDataToTldLinks = new MetaDataToTldLinks();
    }
    
    public void map(Text url, Text metadataText, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
      try {
        
        // from link
        String fromLink = MetaDataToTldLinks.tldOf(url.toString());        
        if (fromLink == null) {
          reporter.getCounter("fromLink", "wasNull").increment(1);
          return;
        }
        
        // outbound links
        ParseResult toLinks = metaDataToTldLinks.outboundLinks(metadataText.toString());
        reporter.getCounter("toLink", "wasNull").increment(toLinks.numNull);
        
        // emit combos
        for (String toLink : toLinks.links) {
          if (!toLink.equals(fromLink)) {
            collector.collect(new Text(fromLink+" "+toLink), ONE);
          }
        }
        
      }
      catch (Exception e) {
        reporter.getCounter("error", e.getClass().getName()).increment(1);
      }
    }
    
  }
  
  public static class SumReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text fromToLink, Iterator<IntWritable> counts, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
      int sum = 0;
      
      while (counts.hasNext())
        sum += counts.next().get();
      
      collector.collect(fromToLink, new IntWritable(sum));
    }
    
  }
  
}
