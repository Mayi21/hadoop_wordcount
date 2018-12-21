package Practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Case01 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// write your code here

		String[] path = new String[2];
		path[0] ="D:\\Study\\JAVA\\idea\\input\\input1";
		path[1] = "D:\\Study\\JAVA\\idea\\output\\output1";
		Configuration configuration = new Configuration();

		if (path.length != 2) {
			System.err.println("Usage:wordcount <input><output>");
			System.exit(2);
		}

		Job job = new Job(configuration, "word count");

		job.setJarByClass(Case01.class);
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(path[0]));
		FileOutputFormat.setOutputPath(job, new Path(path[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	private static class TempMapper extends Mapper<LongWritable, Text, Text, Text> {
		private int map_read = 0;
		private int map_write = 0;
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
			map_read ++;
			String[] toks = value.toString().trim().split(" ");
			for (int i = 0;i < toks.length;i++){
				map_write ++;
				context.write(new Text(toks[i]),new Text(""));
			}
		}
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			context.getCounter("MYCOUNTER","map-write").setValue(map_write);
			context.getCounter("MYCOUNTER","map_read").setValue(map_read);

		}
	}
	private static class TempReducer extends Reducer<Text, Text, Text, Text> {
		private int reduce_read_key = 0;
		private int reduce_read_value = 0;
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
			int count = 0;
			reduce_read_key ++;
			for (Text value : values) {
				count++;
				reduce_read_value ++;
			}
			context.write(new Text(key.toString()), new Text(""));
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			context.getCounter("MYCOUNTER","reduce-write").setValue(reduce_read_key);
			context.getCounter("MYCOUNTER","reduce-read-value").setValue(reduce_read_value);
			context.getCounter("MYCOUNTER","reduce-read-key").setValue(reduce_read_key);
		}
	}

}
