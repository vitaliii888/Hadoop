import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class mm {


    public static class DividerMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
            String tag = itr.nextToken();
            Configuration conf = context.getConfiguration();

            int groups = conf.getInt("mm.groups",10);
            int nrows = conf.getInt("mm.row-count",10);
            int width = nrows/groups;

            int i,j;
            double v;
            i=Integer.parseInt(itr.nextToken());
            j=Integer.parseInt(itr.nextToken());
            v=Double.parseDouble(itr.nextToken());


            if(tag.equals("A")){
                int I=i/width,K=j/width;
                for(int J=0;J<groups;J++){
                    context.write(new Text(I + "\t" + J + "\t" + K),value);
                }
            }else{
                int J=j/width,K=i/width;
                for(int I=0;I<groups;I++)
                    context.write(new Text(I + "\t" + J + "\t" + K),value);
            }

        }
    }

    

    public static class ProductReducer
            extends Reducer<Text,Text,Text,DoubleWritable> {
        private HashMap<String, Double> matrixA;
        private HashMap<String, Double> matrixB;

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int width = conf.getInt("mm.row-count",10)/conf.getInt("mm.groups",10);

            matrixA = new HashMap<String, Double>(2*width*width,0.75f);
            matrixB = new HashMap<String, Double>(2*width*width,0.75f);

            StringTokenizer itrk = new StringTokenizer(key.toString(),"\t");
            int I = Integer.parseInt(itrk.nextToken());
            int J = Integer.parseInt(itrk.nextToken());

            for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString(),"\t");
                String tag = itr.nextToken();
                int i = Integer.parseInt(itr.nextToken());
                int j = Integer.parseInt(itr.nextToken());
                double v = Double.parseDouble(itr.nextToken());
                
                i%=width;
                j%=width;
                String hash_key = new String(i + "\t" + j);

                if(tag.equals("A")){
                    matrixA.put(hash_key, new Double(v));
                }else{
                    matrixB.put(hash_key, new Double(v));
                }
            }


            for(int i=0;i<width;i++)
                for(int j=0;j<width;j++){
                    double curr_elem=0;
                    for(int k=0;k<width;k++){
                        String first_key = new String(i + "\t" + k);
                        String second_key = new String(k + "\t" + j);
                        if(matrixA.containsKey(first_key) &&
                                matrixB.containsKey(second_key))
                            curr_elem+=matrixA.get(first_key)*matrixB.get(second_key);
                    }
                    //coordinates in a big C matrix
                    int c_i=i+I*width,c_j=j+J*width;
                    if(curr_elem!=0.0)
                        context.write(new Text(c_i+"\t"+c_j),new DoubleWritable(curr_elem));
                }

        }
    }

    public static class SummarizerMapper
            extends Mapper<Text, DoubleWritable, Text, DoubleWritable>{

        public void map(Text key, DoubleWritable value, Context context
        ) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class SummarizerReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            if(sum!=0.0) {
                result.set(sum);
                context.write(new Text("C\t" + key.toString()), result);
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: mm <path to matrix A> <path to matrix B> <path to result>");
            System.exit(-1);
        }

        Path outputPath = new Path(otherArgs[2]);
        outputPath.getFileSystem(conf).delete(outputPath, true);    //delete the output path if it exists
        Path temp = new Path("./temp");
        temp.getFileSystem(conf).delete(temp,true);

        FileSystem fs = FileSystem.get(URI.create(otherArgs[0]), conf);
        Path pt = new Path(otherArgs[0] + "/size");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String content = new String();

        try {
            content = reader.readLine();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }

        Integer nrows, ncolumns;
        StringTokenizer itr = new StringTokenizer(content,"\t");
        nrows=Integer.parseInt(itr.nextToken());
        ncolumns=Integer.parseInt(itr.nextToken());

        conf.setInt("mm.row-count",nrows);
        conf.setInt("mm.column-count",ncolumns);


        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(otherArgs[2] + "/size"))));
        content = nrows + "\t" + ncolumns;
        try {
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writer.close();
        }


        Job job1 = new Job(conf, "mm1");
        job1.setJarByClass(mm.class);
        job1.setMapperClass(DividerMapper.class);
        job1.setNumReduceTasks(conf.getInt("mm.num-reducers",1));
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(ProductReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1,new Path(otherArgs[0]+"/data"));
        FileInputFormat.addInputPath(job1,new Path(otherArgs[1]+"/data"));
        FileOutputFormat.setOutputPath(job1, new Path("./temp"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = new Job(conf, "mm2");
        job2.setJarByClass(mm.class);
        job2.setNumReduceTasks(conf.getInt("mm.num-reducers",1));
        job2.setMapperClass(SummarizerMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setReducerClass(SummarizerReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2,new Path("./temp"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]+"/data"));


        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}