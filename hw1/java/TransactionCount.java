import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.StringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TransactionCount {
	public static class TransactionCountMapper
			extends Mapper<LongWritable, Text, Text, IntWritable>{

			private Text trans = new Text();
			private final static IntWritable one = new IntWritable(1);

			public void map(LongWritable key, Text value, Context context
					) throws IOException, InterruptedException {
				context.write(value, one);
					}
	}

	public static class TransactionCountReducer
			extends Reducer<Text, IntWritable, Text, NullWritable>{

			public void reduce(Text key, Iterable<IntWritable> values, Context context
					) throws IOException, InterruptedException{
				context.write(key, NullWritable.get());
					}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Transaction Intersection Merge");
		job.setJarByClass(TransactionCount.class);
		job.setMapperClass(TransactionCountMapper.class);
		job.setReducerClass(TransactionCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
