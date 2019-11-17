package DistinctFilteringByGrade;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
                          Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }

    @Override
    public void run(Reducer<Text, NullWritable, Text, NullWritable>.Context arg0)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.run(arg0);
    }

    @Override
    protected void setup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
    }

}