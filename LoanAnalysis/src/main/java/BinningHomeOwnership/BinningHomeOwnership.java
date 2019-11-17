package BinningHomeOwnership;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class BinningHomeOwnership {
    public static class TMapper extends Mapper<Object, Text, Text, NullWritable>
    {

        private MultipleOutputs<Text, NullWritable> mos=null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            mos = new MultipleOutputs(context);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {

            String line = value.toString();
            String row[] = value.toString().split(",");

            if(row.length > 15 && !line.contains("home_ownership") ){
                String cancellationCode = row[11];

                // System.out.println(cancellationCode);
                if(cancellationCode.contains("MORTGAGE"))
                    mos.write("bins", value, NullWritable.get(),"loans-mortgage-home");
                if(cancellationCode.contains("OWN"))
                    mos.write("bins", value, NullWritable.get(),"loans-own-home");
                if(cancellationCode.contains("RENT"))
                    mos.write("bins", value, NullWritable.get(),"loans-rent-home");

            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            mos.close();
        }

    }







    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cancellation Binning ");
        job.setJarByClass(BinningHomeOwnership.class);
        job.setMapperClass(TMapper.class);


        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}