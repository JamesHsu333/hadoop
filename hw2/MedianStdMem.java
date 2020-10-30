import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MedianStdMem {

    public static class MedianStdDevTuple implements Writable{
        private float stdDev=0;
        private float median=0;
        private int rate=0;

        public void setStdDev(float stdDev){
            this.stdDev=stdDev;
        }

        public double getStdDev(){
            return stdDev;
        }

        public void setMedian(float median){
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
            stdDev=in.readFloat();
            median=in.readFloat();
            rate=in.readInt();

        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeFloat(this.stdDev);
            out.writeFloat(this.median);
            out.writeInt(this.rate);

        }
        public String toString(){
            return "medianValue:"+median+"  "+"stdDev:"+stdDev;
        }
    }

    public static class MedianStdMemMapper
            extends Mapper<LongWritable, Text, Text, SortedMapWritable>{

            private Text hour = new Text();
            private IntWritable commentLength = new IntWritable();
            private final LongWritable ONE=new LongWritable(1);


            public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
                int ctr = 0;
                StringTokenizer itr = new StringTokenizer(value.toString());
                while(itr.hasMoreTokens()) {
                    if(ctr==0){
                        hour.set(itr.nextToken());
                    }else{
                        commentLength.set(itr.nextToken().length());
                    }
                    ctr++;
                }
                if(ctr==1){
                    commentLength.set(0);
                }
                SortedMapWritable outCommentLength = new SortedMapWritable();
                outCommentLength.put(commentLength, ONE);
                context.write(hour, outCommentLength);
                    }
    }

    public static class MedianStdMemCombiner
            extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {
            protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
               SortedMapWritable outValue = new SortedMapWritable();
                for (SortedMapWritable v : values) {
                    for (Map.Entry<WritableComparable, Writable> entry : v.entrySet()) {
                        LongWritable count = (LongWritable) outValue.get(entry.getKey());
                        if (count != null) { count.set(count.get()
                                + ((LongWritable) entry.getValue()).get());
                        } else {
                            outValue.put(entry.getKey(), new LongWritable( ((LongWritable) entry.getValue()).get()));
                        }
                    }
                }
                context.write(key, outValue);
            }
        }

    public static class MedianStdMemReducer
            extends Reducer<Text, SortedMapWritable, Text, MedianStdDevTuple>{

            private MedianStdDevTuple result = new MedianStdDevTuple();
            private TreeMap<Integer, Long> commentLengths = new TreeMap<Integer, Long>();

            public void reduce(Text key, Iterable<SortedMapWritable> values, Context context
                    ) throws IOException, InterruptedException{
                float sum = 0;
                long totalComments = 0; commentLengths.clear(); result.setStdDev(0);result.setMedian(0);

                for (SortedMapWritable v : values) {
                    for (Map.Entry<WritableComparable, Writable> entry : v.entrySet()) {
                        int length = ((IntWritable) entry.getKey()).get();
                        long count = ((LongWritable) entry.getValue()).get();
                        totalComments += count;
                        sum += length * count;
                        Long storedCount = commentLengths.get(length);
                        if (storedCount == null) {
                            commentLengths.put(length, count);
                        } else {
                            commentLengths.put(length, storedCount + count);
                        }
                    }
                }

                long medianIndex = totalComments / 2L;
                long previousComments = 0;
                long comments = 0;
                int prevKey = 0;
                for (Map.Entry<Integer, Long> entry : commentLengths.entrySet()) { 
                    comments = previousComments + entry.getValue();
                    if ((previousComments <= medianIndex) && (medianIndex < comments)) {
                        if (totalComments % 2 == 0 && previousComments == medianIndex) { 
                            result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
                        } else {
                            result.setMedian(entry.getKey());
                        }
                        break;
                    }
                    previousComments = comments; 
                    prevKey = entry.getKey();
                }
                // calculate standard deviation
                float mean = sum / totalComments;
                float sumOfSquares = 0.0f;
                for (Map.Entry<Integer, Long> entry : commentLengths.entrySet()) { 
                    sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) *
                        entry.getValue();
                }
                result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1))); 
                context.write(key, result);
                    }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Median Std Mem");
        job.setJarByClass(MedianStdMem.class);
        job.setMapperClass(MedianStdMemMapper.class);
        job.setCombinerClass(MedianStdMemCombiner.class);
        job.setReducerClass(MedianStdMemReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
