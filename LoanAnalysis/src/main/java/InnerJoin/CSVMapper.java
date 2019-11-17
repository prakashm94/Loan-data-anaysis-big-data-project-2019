package InnerJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        Text t1 = new Text(fields[0]);
        String val = "M"+fields[1]+","+fields[2]+","+fields[3];
        Text t2 = new Text(val);

        context.write(t1,t2);
    }
}
