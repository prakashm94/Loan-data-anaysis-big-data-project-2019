package InvertedIndex;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private BloomFilter<String> origin;

    Funnel<String> p = new Funnel<String>() {

        public void funnel(String from, Sink into) {

            into.putString(from, Charsets.UTF_8);

        }
    };

    @Override
     protected void setup(Context context) throws IOException, InterruptedException {

        this.origin = BloomFilter.create(p, 300000, 0.1);

        String p1 = "MA";
        String p2 = "NY";
       // Person p2 = new Person("Jamie", "Scott");

        ArrayList<String> originList = new ArrayList<String>();
        originList.add(p1);
        originList.add(p2);

        for (String ps : originList) {
            origin.put(ps);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String input = value.toString();

           String splitString[] = input.split(",");
           String origins = splitString[19];
           //System.out.println(origins);
           //String values[] = value.toString().split(",");
           //Person p = new Person(values[1], values[2]);

           if (origin.mightContain(origins) && !splitString[0].contains("id")) {
               context.write(value,NullWritable.get());
           }

       } catch(Exception e) {
           System.out.println(e);
       }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        super.cleanup(context);
    }
}
