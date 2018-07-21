import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Binning {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private DoubleWritable averageRate = new DoubleWritable(0);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        if(tokens.length == 2)
        {
        	averageRate.set(Double.parseDouble(tokens[1]));  
	        if(averageRate.get()>=0 && averageRate.get()<0.5)
	        	context.write(new IntWritable(1), one);
	        else if(averageRate.get()>=0.5 && averageRate.get()< 1)
	        	context.write(new IntWritable(2), one);
	        else if(averageRate.get()>=1 && averageRate.get()< 1.5)
	        	context.write(new IntWritable(3), one);
	        else if(averageRate.get()>=1.5 && averageRate.get()< 2)
	        	context.write(new IntWritable(4), one);
	        else if(averageRate.get()>=2 && averageRate.get()< 2.5)
	        	context.write(new IntWritable(5), one);
	        else if(averageRate.get()>=2.5 && averageRate.get()< 3)
	        	context.write(new IntWritable(6), one);
	        else if(averageRate.get()>=3&& averageRate.get()< 3.5)
	        	context.write(new IntWritable(7), one);
	        else if(averageRate.get()>=3.5 && averageRate.get()< 4)
	        	context.write(new IntWritable(8), one);
	        else if(averageRate.get()>=4 && averageRate.get()< 4.5)
	        	context.write(new IntWritable(9), one);
	        else if(averageRate.get()>=4.5 && averageRate.get()<= 5)
	        	context.write(new IntWritable(10), one);
        }
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val : values) {
            count++;
        }
        context.write(new Text("<"+key), new Text(count+">"));
    }
 }
        
 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    Job job = new Job(conf, "binning");
    job.setJarByClass(Binning.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("/usr/hduser/output_p1/part-r-00000"));
    FileOutputFormat.setOutputPath(job, new Path("/usr/hduser/output_p2/"));
    job.waitForCompletion(true);
    
 }
        
}