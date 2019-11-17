package DistinctFilteringByGrade;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistinctMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String[] tokens = line.split(",");
        if(tokens.length > 15 && !line.contains("grade")) {
            context.write(new Text(tokens[8]), NullWritable.get()); //10
        }

    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    public void run(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.run(context);
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
    }


}