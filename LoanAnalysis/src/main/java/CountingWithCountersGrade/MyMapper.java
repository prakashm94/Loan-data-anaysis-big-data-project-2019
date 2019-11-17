package CountingWithCountersGrade;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class MyMapper extends Mapper<LongWritable, Text, NullWritable,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String grades[]= {"A","B","C","D","E","F", "G"};
        HashSet<String> gradesHashSet= new HashSet<String>(Arrays.asList(grades));

        String input = value.toString();
        String splitString[] = input.split(",");
        if(splitString.length > 8) {
            String grade = splitString[8];
            //  System.out.print(month);

            if (gradesHashSet.contains(grade) && splitString[8] != "grade") {
                // System.out.print(month);
                context.getCounter("MonthCounter", grade).increment(1);
            } else {
                context.getCounter("MonthCounter", "unknown").increment(1);

            }
        }


    }
}
