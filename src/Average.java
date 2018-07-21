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
        
public class Average {
        
 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text movie = new Text();
    private DoubleWritable rate = new DoubleWritable(0);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("::");
        movie.set(tokens[1]);
        rate.set(Double.parseDouble(tokens[2]));        
        context.write(movie, rate);
    }
 } 
        
 public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {
        double average = 0;
        int size = 0;
        for (DoubleWritable val : values) {
            average += val.get();
            size++;
        }
        average = average / size;
        context.write(key, new DoubleWritable(average));
    }
 }
        
 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "average");
    job.setJarByClass(Average.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);	
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("/usr/hduser/input_p1/ratings.dat"));
    FileOutputFormat.setOutputPath(job, new Path("/usr/hduser/output_p1/"));
    job.waitForCompletion(true);
    
 }
        
}