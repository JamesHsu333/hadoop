import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MedianStd {

    public static class MedianStdDevTuple implements Writable{
        private double stdDev=0;
        private double median=0;
        private int rate=0;

        public void setStdDev(double stdDev){
            this.stdDev=stdDev;
        }

        public double getStdDev(){
            return stdDev;
        }

        public void setMedian(double median){
            this.median=median;
        }

        public double getMedian(){
            return median;
        }

        public void setRate(int rate){
            this.rate=rate;
        }

        public double getRate(){
            return rate;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            stdDev=in.readDouble();
            median=in.readDouble();
            rate=in.readInt();

        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(this.stdDev);
            out.writeDouble(this.median);
            out.writeInt(this.rate);

        }
        public String toString(){
            return "medianValue:"+median+"  "+"stdDev:"+stdDev;
        }


    }

    public static class MedianStdMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

            private Text hour = new Text();
            private IntWritable comment = new IntWritable();

            public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
                int ctr = 0;
                StringTokenizer itr = new StringTokenizer(value.toString());
                while(itr.hasMoreTokens()) {
                    if(ctr==0){
                        hour.set(itr.nextToken());
                    }else{
                        comment.set(itr.nextToken().length());
                    }
                    ctr++;
                }
                if(ctr==1){
                    comment.set(0);
                }
                context.write(hour, comment);
                    }
    }

    public static class MedianStdReducer
            extends Reducer<Text, IntWritable, Text, MedianStdDevTuple>{

            private MedianStdDevTuple result = new MedianStdDevTuple();
            private ArrayList<Float> commentLengths = new ArrayList<Float>();

            public void reduce(Text key, Iterable<IntWritable> values, Context context
                    ) throws IOException, InterruptedException{
                float sum = 0;
                float count = 0; commentLengths.clear(); result.setStdDev(0);
                // Iterate through all input values for this key
                for (IntWritable val : values) { commentLengths.add((float) val.get()); sum += val.get();
                    ++count;
                }
                // sort commentLengths to calculate median
                Collections.sort(commentLengths);
                // if commentLengths is an even value, average middle two elements
                if (count % 2 == 0) { result.setMedian((commentLengths.get((int) count / 2 - 1) +
                            commentLengths.get((int) count / 2)) / 2.0f);
                } else {
                    // else, set median to middle value
                    result.setMedian(commentLengths.get((int) count / 2));
                }
                // calculate standard deviation
                float mean = sum / count;
                float sumOfSquares = 0.0f;
                for (Float f : commentLengths) { sumOfSquares += (f - mean) * (f - mean);
                }
                result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1))); 
                context.write(key, result);

                    }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Median Std");
        job.setJarByClass(MedianStd.class);
        job.setMapperClass(MedianStdMapper.class);
        job.setReducerClass(MedianStdReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
