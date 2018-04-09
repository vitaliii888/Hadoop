import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class NewInputFormat
    extends InputFormat<Text,DoubleWritable>{
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>(0);
        Configuration conf = jobContext.getConfiguration();
        long num = conf.getLong("mgen.num-mappers",1);
        long step = conf.getLong("mgen.row-count",2)/num;
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        for(long i=0;i<num;i++){
            EmptySplit s = new EmptySplit();
            s.setStart_line(new LongWritable(i*step));
            if(i==num-1)
                s.setEnd_line(new LongWritable(conf.getInt("mgen.row-count",2)-1));
            else
                s.setEnd_line(new LongWritable((i+1)*step-1));
            s.setSeed(new LongWritable(rand.nextLong()));
            splits.add(s);
        }
        return (List)splits;
    }

    @Override
    public RecordReader<Text, DoubleWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        double sparsity = conf.getDouble("mgen.sparsity",0.5);
        if(sparsity<=0.5) {
            NewRecordReader reader = new NewRecordReader();
            reader.initialize(split, context);
            return reader;
        }else {
            OtherRecordReader reader = new OtherRecordReader();
            reader.initialize(split,context);
            return reader;
        }
    }
}