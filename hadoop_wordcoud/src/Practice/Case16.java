package Practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.jar.Attributes.Name;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jackson.map.deser.std.CalendarDeserializer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.io.IOException;

public class Case16 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        String[] path = new String[2];
        path[0] ="D:\\Study\\JAVA\\idea\\output\\output14";
        path[1] = "D:\\Study\\JAVA\\idea\\output\\output16";
        Configuration configuration = new Configuration();

        if (path.length != 2) {
            System.err.println("Usage:wordcount <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration, "word count");

        job.setJarByClass(Case16.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(path[0]));
        FileOutputFormat.setOutputPath(job, new Path(path[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class TempMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        };
        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {
            try{
                System.load("D:\\DOWNLOAD\\hadoop-2.8.5\\bin\\hadoop.dll");
            }catch (Throwable t){
            }
            String[] toks = value.toString().trim().split("\t");
            context.write(new Text(toks[0]), new Text(toks[2]));
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    private static class TempReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        }
        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for(Text value : values){
                total ++;
            }
            context.write(new Text(key.toString()),new Text(String.valueOf(total)));
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);

        }
    }
}
