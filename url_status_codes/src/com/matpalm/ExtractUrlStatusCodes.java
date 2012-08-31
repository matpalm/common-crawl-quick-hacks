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

/**
 * a simple demo of extracting crawler fetch status codes from the meta data data set.
 * commented heavily enough to make my eyes bleed. 
 */
public class ExtractUrlStatusCodes extends Configured implements Tool {
  
  /**
   * we always include a main as a simple way to be able to run directly in an ide without the hadoop stack 
   */
  public static void main(String args[]) throws Exception {
    ToolRunner.run(new ExtractUrlStatusCodes(), args);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length!=2) {
      throw new RuntimeException("usage: "+getClass().getName()+" <input> <output>");
    }
    
    // let's configure the job
    JobConf conf = new JobConf(getConf(), getClass());
    
    // a name is always handy for viewing on the console
    conf.setJobName(getClass().getName());
    
    // we need to specify some type of what input/output key/values we'll be dealing with
    // it's all text in this example
    conf.setMapOutputKeyClass(Text.class);   
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
        
    // we'll be doing a map only job, ie no need for reducers.
    conf.setMapperClass(ExtractUrlStatusCodesMapper.class);    
    conf.setNumReduceTasks(0); // map only    
        
    // input is is sequence file and we'll read/write based on the command line args specified
    conf.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    // if we want to we can optionally configure the
    // job to output as sequence files, block compressed (snappy)
    // if the next step in our pipeline is another hadoop job this is always a good idea
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

    // a parser for handling the json metadata
    private JsonParser parser;
    
    // we use configure as a sort-a constructor to build the parser
    // it's often handy to do different setup based on the job conf (though we don't in this case) 
    public void configure(JobConf job) {
      super.configure(job);
      parser = new JsonParser();
    }
    
    public void map(Text url, Text metadataText, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
      try {
        // parse the meta data
        JsonObject metaData = parser.parse(metadataText.toString()).getAsJsonObject();
        // extract the response
        String response = metaData.getAsJsonObject("http_headers").get("response").getAsString();
        // reemit the url and response
        collector.collect(url, new Text(response));
      }
      catch (Exception e) {
        // *ALWAYS* catch exceptions, and in this case we just keep track of the counts
        reporter.getCounter("error", e.getClass().getName()).increment(1);
      }
    }
    
  }
  
}
