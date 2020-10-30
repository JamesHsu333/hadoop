import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.StringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Transaction {
	public static class TransactionMapper
			extends Mapper<LongWritable, Text, Text, Text>{

			private Text item = new Text();

			public void map(LongWritable key, Text value, Context context
					) throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString());
				while(itr.hasMoreTokens()) {
					item.set(itr.nextToken());
					context.write(item, value);
				}
					}
	}

	public static class TransactionReducer
			extends Reducer<Text, Text, Text, Text>{
				
				private Text results = new Text();

			public void reduce(Text key, Iterable<Text> values, Context context
					) throws IOException, InterruptedException{
				ArrayList<String> value = new ArrayList<String>();
				Set<Character> set = new HashSet<Character>();
				Set<Character> com = new HashSet<Character>();
				StringBuilder intersection = new StringBuilder();
				for(Text val:values){
					value.add(val.toString());
				}
				for(int i=0; i<value.size(); i++) {
					for (int j=i+1; j<value.size(); j++) {
						for(Character ch:value.get(i).toCharArray()){
							if(ch != ' '){
								set.add(ch);
							}
						}
						for(Character ch:value.get(j).toCharArray()){
							if(ch != ' '){
								com.add(ch);
							}
						}
						set.retainAll(com);
						if(!set.isEmpty()){
							Iterator iterator = set.iterator();
							while(iterator.hasNext()){
								intersection.append(" ");
								intersection.append(iterator.next().toString());
							}
							results.set(intersection.toString());
							context.write(key, results);
						}
						intersection.setLength(0);
						set.clear();
						com.clear();
					}
				}
					}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Transaction Intersection");
		job.setJarByClass(Transaction.class);
		job.setMapperClass(TransactionMapper.class);
		job.setReducerClass(TransactionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
