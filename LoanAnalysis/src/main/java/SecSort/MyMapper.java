package SecSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value,
                       Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] tokens = line.split(",");
		String grade = null;
		String subbgrade = null;
	if(!value.toString().contains("member_id") && tokens.length>14)  {
	try {

		grade = tokens[8];
		subbgrade = tokens[9];

	} catch (Exception e) {

	}

	if (grade != null && subbgrade != null) {

		CompositeKeyWritable outKey = new CompositeKeyWritable(grade, subbgrade);

		context.write(outKey, NullWritable.get());

	}
	}
	}

}
