import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.*;


class NewRecordReader extends RecordReader<Text, DoubleWritable> {
    Configuration conf = new Configuration();
    private long start_line = 0;
    private long end_line = 0;
    private long seed = 0;
    private double min = 0.0;
    private double max = 1.0;
    private double sparsity = 0.0;
    private double value = 0.0;
    Random chunkrand;

    private Set<Integer> excl = new HashSet<>();
    private Integer it = new Integer(0);

    private long i=0, j=-1;
    private long ncolumns=2,nrows=2;
    private boolean initialized=false;


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if(!initialized) {
            conf = context.getConfiguration();
            start_line=((EmptySplit)split).getStart_line().get();
            end_line = ((EmptySplit)split).getEnd_line().get();
            seed = ((EmptySplit)split).getSeed().get();
            chunkrand = new Random(seed);

            nrows = end_line - start_line+1;
            ncolumns = conf.getLong("mgen.column-count", 2);
            min = conf.getDouble("mgen.min",0.0);
            max = conf.getDouble("mgen.max",1.0);
            sparsity = conf.getDouble("mgen.sparsity",0.0);
            int total_num = (int)((end_line-start_line+1)*ncolumns);
            int excl_num = (int)(total_num*sparsity);
            //Integer total_num = new Double((end_line-start_line+1)*ncolumns).intValue();
            //Integer excl_num = new Double(total_num*sparsity).intValue();
            while(excl.size() < excl_num) {
                excl.add(chunkrand.nextInt(total_num-1));
            }
            initialized=true;
        }
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        value=excl.contains(it)?0.0:min+chunkrand.nextDouble()*(max-min);
        while(!excl.contains(it) && value==0.0)
            value=min+chunkrand.nextDouble()*(max-min);
        it++;
        if(j<ncolumns-1 && i<nrows){
            j++;
            return true;
        }else if(j==ncolumns-1 && i<nrows-1) {
            i++;
            j=0;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        long pos = i+start_line;
        return new Text(pos +"\t"+j);
    }

    @Override
    public DoubleWritable getCurrentValue() throws IOException,
            InterruptedException {
        return new DoubleWritable(value);
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}