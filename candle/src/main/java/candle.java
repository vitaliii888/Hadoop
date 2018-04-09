import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class candle {

    public static class FilterMapper
            extends Mapper<Object, Text, Text, CompositeValue>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            /*Regexp to choose instrument*/
            String regexp = conf.get("candle.securities",".*");
            Pattern pattern = Pattern.compile(regexp);

        /*Time borders*/
            String start_date = conf.get("candle.date.from","19000101");
            String finish_date = conf.get("candle.date.to","20200101");


        /*Time borders*/
            String start_time = conf.get("candle.time.from","1000");
            start_time+="00000";
            Integer start_hour = Integer.parseInt(start_time.substring(0,2));
            Integer start_min = Integer.parseInt(start_time.substring(2,4));
            String finish_time = conf.get("candle.time.to","1800");
            finish_time+="00000";
        }
    }

    public static class SumReducer
            extends Reducer<Text,CompositeValue,Text,Text> {
        private Text v = new Text();
        MultipleOutputs<Text,Text> mos;

        @Override
        public void setup(Context context){
            mos = new MultipleOutputs(context);
        }


        public void reduce(Text key, Iterable<CompositeValue> values,
                           Context context
        ) throws IOException, InterruptedException {

            String open_time=new String("");
            Long open_id=0l;
            Double open_price=0.0;

            Double high_price=0.0,low_price=0.0;

            String close_time=new String("");
            Long close_id=0l;
            Double close_price=0.0;

            boolean first_line = true;
            for (CompositeValue val : values) {
                if(first_line){
                    first_line=false;
                    open_time=close_time=val.getTIME();
                    open_id=close_id=val.getID_DEAL();
                    open_price=high_price=low_price=close_price=val.getPRICE_DEAL();
                    continue;
                }

                String time=val.getTIME();
                Long id=val.getID_DEAL();
                Double price=val.getPRICE_DEAL();

                /*Open position*/
                if(time.compareTo(open_time)<0 || time.compareTo(open_time)==0 && id<open_id){
                    open_time=time;
                    open_id=id;
                    open_price=price;
                }
                if(price>high_price)
                    high_price=price;
                if(price<low_price)
                    low_price=price;
                if(time.compareTo(close_time)>0 || time.compareTo(close_time)==0 && id>close_id){
                    close_time=time;
                    close_id=id;
                    close_price=price;
                }
            }

            v.set(String.format("%.1f,%.1f,%.1f,%.1f",open_price,high_price,low_price,close_price));

            StringTokenizer itr = new StringTokenizer(key.toString(),",");
            String symbol = itr.nextToken();

            mos.write(symbol,key,v);
            //context.write(key, v);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            mos.close();
        }
    }





    public static void main(String[] args) throws Exception {








        Text key = new Text();
        CompositeValue value = new CompositeValue();
        File infile = new File(otherArgs[0]);
        SequenceFile.Writer writer = null;



        /*Generate set of strings to generate multiple output????*/
        HashSet<String> set = new HashSet<>();

        FileSystem fs = FileSystem.get(URI.create(otherArgs[0]),conf);
        FSDataInputStream in = null;

        try {
            in = fs.open(new Path(otherArgs[0]));
            //new FileReader(file)
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            System.out.println("File assigned");

            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(seqFilePath),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()));

            boolean first_line = true;
            for (String line = br.readLine(); line!=null; line = br.readLine()) {
                if(first_line){
                    first_line=false;
                    continue;
                }
                StringTokenizer itr = new StringTokenizer(line,",");

                /*DROP UNNECESSARY DATA*/
                //SYMBOL
                String symbol = itr.nextToken();
                Matcher matcher = pattern.matcher(symbol);
                //choose appropriate instrument
                if(!matcher.find())
                    continue;

                //SYSTEM
                itr.nextToken();

                //DATE
                String date = itr.nextToken();
                //necessary for key generation
                Integer hour = Integer.parseInt(date.substring(8,10));
                Integer min = Integer.parseInt(date.substring(10,12));

                //date borders
                if(date.substring(0,8).compareTo(start_date)<0||date.substring(0,8).compareTo(finish_date)>=0)
                    continue;
                //time borders
                if(date.substring(8,17).compareTo(start_time)<0||date.substring(8,17).compareTo(finish_time)>=0)
                    continue;

                set.add(symbol);

                /*KEY for TotalOrderParttioner*/
                /*CANDLE'S MOMENT*/
                Long width = conf.getLong("candle.width",3600000);
                Integer mins_diff = min-start_min;
                Integer hours_diff=0;
                if(mins_diff<0){
                    mins_diff+=60;
                    hours_diff-=1;
                }
                hours_diff+=hour-start_hour;
                Integer candle_num = (int)((hours_diff*60+mins_diff)*60*1000/width);


                /*time shift*/
                Long sh_time = candle_num*width;
                sh_time/=60000;
                Integer sh_min=(int)(sh_time%60);
                Integer sh_hour=(int)(sh_time/60);

                /*Candle start moment*/
                Integer candle_st_min = (start_min + sh_min)%60;
                Integer candle_st_hour = start_hour+sh_hour+(start_min + sh_min)/60;


                String moment = date.substring(0,8);
                if(candle_st_hour<10)
                    moment+="0";
                moment+=candle_st_hour.toString();
                if(candle_st_min<10)
                    moment+="0";
                moment+=candle_st_min.toString();
                moment+="00000";

                key.set(symbol+","+moment);


                /*VALUE for TotalOrderPartitioner*/
                //ID_DEAL
                Long id_deal = Long.parseLong(itr.nextToken());
                //PRICE_DEAL
                Double price_deal = Double.parseDouble(itr.nextToken());

                value.set(date.substring(8,17),id_deal,price_deal);


                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }

        /*STEP 1: DATA PREPROCESSING*/

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: candle <inputDir> <outputDir>");
            System.exit(-1);
        }

        /*REMOVING EXISTING FILES*/
        String uri = new String("input.seqfile");
        Path seqFilePath = new Path(uri);
        seqFilePath.getFileSystem(conf).delete(seqFilePath,true);
        Path outputDir = new Path(otherArgs[1]);
        outputDir.getFileSystem(conf).delete(outputDir, true);
        Path partitionOutputPath = new Path("partition.seqfile");
        partitionOutputPath.getFileSystem(conf).delete(partitionOutputPath,true);
        Path inputDir = new Path(otherArgs[0]);


        Job pre_job = new Job();
        pre_job.setJarByClass(candle.class);

        pre_job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(pre_job,inputDir);
        pre_job.setNumReduceTasks(0);
        pre_job.setMapOutputKeyClass(Text.class);
        pre_job.setMapOutputValueClass(CompositeValue.class);
        pre_job.setOutputFormatClass(SequenceFileOutputFormat.class);

        pre_job.setMapperClass(FilterMapper.class);
        pre_job.setOutputKeyClass(Text.class);
        pre_job.setOutputValueClass(CompositeValue.class);




        /*STEP 2: PROCESSING FILTERED DATA USING TotalOrderPartitioner*/

        /*SET COMMA AS A KEY/VALUE OUTPUT SEPARATOR*/
        conf.set("mapreduce.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "candle");
        job.setJarByClass(candle.class);


        // The following instructions should be executed before writing the partition file
        Integer num_reducers = conf.getInt("candle.num.reducers",1);
        job.setNumReduceTasks(num_reducers);
        FileInputFormat.setInputPaths(job, seqFilePath);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CompositeValue.class);

        // Write partition file with random sampler
        InputSampler.Sampler<Text, CompositeValue> sampler = new InputSampler.RandomSampler<>(0.1, 10, 100);
        InputSampler.writePartitionFile(job, sampler);

        // Use TotalOrderPartitioner and default identity mapper and reducer
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        FileOutputFormat.setOutputPath(job, outputDir);

        for(String s : set){
            System.out.print(s+"\n");
            MultipleOutputs.addNamedOutput(job,s, TextOutputFormat.class,Text.class,Text.class);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}