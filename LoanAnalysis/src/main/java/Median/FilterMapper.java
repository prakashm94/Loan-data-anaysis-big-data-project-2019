package Median;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String split[]=value.toString().split(",");
          // IntWritable one = new IntWritable(1);
           String  symbol = split[0];
           if(!symbol.contains("member_id") && split.length>20 && !split[5].equalsIgnoreCase("0") && !split[20].equalsIgnoreCase("") && !split[20].equalsIgnoreCase("")) {
               context.write(new Text(split[8]),new Text(split[24]));
           }
       } catch(Exception ex) {
           System.out.println("Exception in mapper"+ex);
       }
    }
}
