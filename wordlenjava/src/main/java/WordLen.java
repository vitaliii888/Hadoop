import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordLen {

    public static class RegexMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Pattern pattern = Pattern.compile("[\\w\\d]+");
        private IntWritable wordLength = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                wordLength.set(matcher.end()-matcher.start());
                context.write(wordLength, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordLen <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word len");
        job.setJarByClass(WordLen.class);
        job.setMapperClass(RegexMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}