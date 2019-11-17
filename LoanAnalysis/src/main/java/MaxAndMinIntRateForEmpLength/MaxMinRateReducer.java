package MaxAndMinIntRateForEmpLength;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxMinRateReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Float currentMaxRateValue;
        Float currentMinRateValue;

        Float maxRateValue = (float) 0;
        Float minRateValue = (float) Integer.MAX_VALUE;

        for (Text value : values) {
String a[]=value.toString().split(" ");
            currentMaxRateValue = Float.parseFloat(a[1].trim().replace("%", ""));
            currentMinRateValue = Float.parseFloat(a[0].trim().replace("%", ""));

            if (currentMaxRateValue > maxRateValue) {
                maxRateValue = currentMaxRateValue;
            }
            if (currentMinRateValue < minRateValue) {
                minRateValue = currentMinRateValue;
            }
        }
      //  String result = "Min Rate: " + String.valueOf(minRateValue) + " Max Rate: "
      //          + String.valueOf(maxRateValue);
        context.write(key, new Text(minRateValue+" "+maxRateValue));
    }
}
