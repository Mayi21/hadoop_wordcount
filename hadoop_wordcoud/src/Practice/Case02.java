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

public class Case02 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// write your code here

		String[] path = new String[2];
		path[0] ="D:\\Study\\JAVA\\idea\\input\\input2";
		path[1] = "D:\\Study\\JAVA\\idea\\output\\output2";
		Configuration configuration = new Configuration();

		if (path.length != 2) {
			System.err.println("Usage:wordcount <input><output>");
			System.exit(2);
		}

		Job job = new Job(configuration, "word count");

		job.setJarByClass(Case02.class);
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
			String[] toks = value.toString().trim().split(" ");
			for (int i = 0;i < toks.length;i++){
				String string = Change(toks[i]);
				context.write(new Text(string),new Text(""));
			}
		}
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		public String Change(String k){
			k = String.valueOf(100 - k.length()) + "," + k;
			return k;
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
			int count = 0;
			for (Text value : values) {
				count++;
			}
			context.write(new Text(key.toString().split(",")[1]), new Text(count + ""));
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

}
