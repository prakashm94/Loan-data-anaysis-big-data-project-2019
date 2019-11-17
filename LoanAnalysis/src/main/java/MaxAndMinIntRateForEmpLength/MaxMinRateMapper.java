package MaxAndMinIntRateForEmpLength;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxMinRateMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text symbol;
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length > 5) {
            if (!line.contains("int_rate")) {
                symbol = new Text(fields[10]);
                context.write(symbol, new Text(fields[6]+" "+fields[6]));
            }

        }
    }
}