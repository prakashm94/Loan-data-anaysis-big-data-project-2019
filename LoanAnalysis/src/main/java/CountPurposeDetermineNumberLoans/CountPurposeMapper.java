package CountPurposeDetermineNumberLoans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountPurposeMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
private static final IntWritable one = new IntWritable(1);

@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {
        String line = value.toString();

        String[] tokens = line.split(",");
        if(tokens.length > 15) {
                //  for(String s : tokens) {
                //  System.out.println(tokens[17]);
                context.write(new Text(tokens[17]), one);
        }
        //}
        }

@Override
protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
        }

@Override
public void run(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.run(context);
        }

@Override
protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
        }


}