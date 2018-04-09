import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmptySplit extends InputSplit implements Writable {

    private LongWritable start_line = new LongWritable(0);
    private LongWritable end_line = new LongWritable(0);
    private LongWritable seed = new LongWritable(0);

    public void setSeed(LongWritable seed) {
        this.seed = seed;
    }

    public LongWritable getSeed() {
        return seed;
    }

    public void setStart_line(LongWritable _start){
        this.start_line=_start;
    }

    public void setEnd_line(LongWritable _end){
        this.end_line=_end;
    }

    public LongWritable getEnd_line() {
        return end_line;
    }

    public LongWritable getStart_line() {
        return start_line;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        start_line.write(dataOutput);
        end_line.write(dataOutput);
        seed.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        start_line.readFields(dataInput);
        end_line.readFields(dataInput);
        seed.readFields(dataInput);
    }
}
