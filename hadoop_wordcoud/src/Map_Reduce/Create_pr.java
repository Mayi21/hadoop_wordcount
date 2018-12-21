package Map_Reduce;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Create_pr {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        String[] path = new String[2];
        path[0] ="D:\\Study\\JAVA\\idea\\output\\Create_data";
        path[1] = "D:\\Study\\JAVA\\idea\\output\\Create_pr";
        Configuration configuration = new Configuration();

        if (path.length != 2) {
            System.err.println("Usage:wordcount <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration, "word count");

        job.setJarByClass(Create_pr.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(path[0]));
        FileOutputFormat.setOutputPath(job, new Path(path[1]));
        //File_op.Rname(path[1] + "\\part-r-00000",path[1] + "\\peoplerank");
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
            context.write(new Text(key.toString()),new Text(String.valueOf(1)));
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);

        }
    }
}
