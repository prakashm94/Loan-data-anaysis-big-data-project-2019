package InvertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String input = value.toString();

           String splitString[] = input.split(",");
           String origin = splitString[24];

           context.write(new Text(origin), new Text(splitString[0]));
       } catch(Exception e) {
           System.out.println(e);
       }
    }
}
