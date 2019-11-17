package MaxAndMinIntRateForEmpLength;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxMinRateDriver {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance();

        job.setJarByClass(MaxMinRateDriver.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setMapperClass(MaxMinRateMapper.class);
        job.setReducerClass(MaxMinRateReducer.class);
        job.setCombinerClass(MaxMinRateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setNumReduceTasks(1);



        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.waitForCompletion(true);
    }
}
