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

public class Case14 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        String[] path = new String[2];
        path[0] ="D:\\Study\\JAVA\\idea\\input\\input14";
        path[1] = "D:\\Study\\JAVA\\idea\\output\\output14";
        Configuration configuration = new Configuration();

        if (path.length != 2) {
            System.err.println("Usage:wordcount <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration, "word count");

        job.setJarByClass(Case14.class);
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
            //String[] toks = value.toString().trim().split("\t");
            try{
                System.load("D:\\DOWNLOAD\\hadoop-2.8.5\\bin\\hadoop.dll");
            }catch (Throwable t){
            }
            context.write(new Text(), new Text());
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    private static class TempReducer extends Reducer<Text, Text, Text, Text> {
        TreeMap<String, String> map = new TreeMap<String, String>();
        private static int size = 1000;
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
            Random random = new Random();
            int i = 1;
            while(i <= 100){
                int a = i;
                int b = random.nextInt(100) + 1;
                String value = String.valueOf(random.nextFloat()).substring(0,5);
                if(a != b){
                    map.put(a + " " + b,value);
                    i++;
                }
            }
            while(true){
                int a = random.nextInt(100) + 1;
                int b = random.nextInt(100) + 1;
                String value = String.valueOf(random.nextFloat()).substring(0,5);
                if(a != b){
                    map.put(a + " " + b,value);
                }
                if(map.size() > 1000){
                    break;
                }
            }

        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            Iterator iterator = map.keySet().iterator();
            while (iterator.hasNext()){
                String key1 = (String)iterator.next();
                String[] dian = key1.split(" ");
                String value = map.get(key1);
                context.write(new Text(dian[0]), new Text(value +"\t" + dian[1]));
            }
        }
    }
}
