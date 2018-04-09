import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class mgen {
    public static class IdMapper
            extends Mapper<Text, DoubleWritable, Text, Text>{

        public void map(Text key, DoubleWritable value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String pattern = conf.get("mgen.float-format","%.3f");
            String tag = conf.get("mgen.tag","");
            if(value.get()!=0.0)
                context.write(new Text(tag+"\t"+key),new Text(String.format(pattern,value.get())));
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: mgen <out>");
            System.exit(1);
        }
        Path outputPath = new Path(otherArgs[0]);
        outputPath.getFileSystem(conf).delete(outputPath, true);    //delete the output path if it exists
        long row_count = conf.getLong("mgen.row-count",2);
        long column_count = conf.getLong("mgen.column-count",2);
        FileSystem fs = FileSystem.get(URI.create(otherArgs[0]), conf);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(otherArgs[0] + "/size"))));
        String content = row_count + "\t" + column_count;
        try {
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writer.close();
        }
        Job job = new Job(conf, "mgen");
        job.setJarByClass(mgen.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(NewInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(IdMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextOutputFormat.setOutputPath(job,new Path(otherArgs[0]+"/data"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}