package AverageInterestRateByGrade;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageInterestRateByGrade {
    public static class MapLoan extends Mapper<LongWritable, Text, Text, LoanAveragingPair> {
        private Text outText = new Text();
        private Map<String,LoanAveragingPair> pairMap = new HashMap<String,LoanAveragingPair>();
        private LoanAveragingPair pair = new LoanAveragingPair();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnValues = line.split(",");

            if (columnValues.length > 6) {
                if (!columnValues[0].contains("id")) {
                    outText.set(columnValues[8]);
                    Float LoanTime = Float.parseFloat(columnValues[6].replace("%", ""));
                    LoanAveragingPair pair = pairMap.get(columnValues[8]);
                    if (pair == null) {
                        pair = new LoanAveragingPair();
                        pairMap.put(columnValues[8], pair);

                    }
                    Float Loan = pair.getLoan().get() + LoanTime;
                    int count = pair.getCount().get() + 1;
                    pair.set(Loan, count);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> keys = pairMap.keySet();
            Text keyText = new Text();
            for (String key : keys) {
                keyText.set(key);
                context.write(keyText,pairMap.get(key));
            }
        }
    }

    public static class Reduce extends Reducer<Text, LoanAveragingPair, Text, FloatWritable> {
        public void reduce(Text key, Iterable<LoanAveragingPair> values
                , Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (LoanAveragingPair val : values) {
                sum += val.getLoan().get();
                count += val.getCount().get();
            }
            context.write(key, new FloatWritable(sum/count));
        }
    }

    public static class LoanAveragingPair implements Writable, WritableComparable<LoanAveragingPair> {
        private FloatWritable Loan;
        private IntWritable count;

        public LoanAveragingPair() {
            set(new FloatWritable(0), new IntWritable(0));
        }

        public LoanAveragingPair(Float Loan, int count) {
            set(new FloatWritable(Loan), new IntWritable(count));
        }


        public void write(DataOutput out) throws IOException {
            Loan.write(out);
            count.write(out);
        }


        public void readFields(DataInput in) throws IOException {
            Loan.readFields(in);
            count.readFields(in);
        }


        public int compareTo(LoanAveragingPair other) {
            int compareVal = this.Loan.compareTo(other.getLoan());
            if (compareVal != 0) {
                return compareVal;
            }
            return this.count.compareTo(other.getCount());
        }
        public void set(Float Loan, int count){
            this.Loan.set(Loan);
            this.count.set(count);
        }

        public void set(FloatWritable Loan, IntWritable count) {
            this.Loan = Loan;
            this.count = count;
        }


        public static LoanAveragingPair read(DataInput in) throws IOException {
            LoanAveragingPair averagingPair = new LoanAveragingPair();
            averagingPair.readFields(in);
            return averagingPair;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LoanAveragingPair that = (LoanAveragingPair) o;

            if (!count.equals(that.count)) return false;
            if (!Loan.equals(that.Loan)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = Loan.hashCode();
            result = 163 * result + count.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LoanAveragingPair{" +
                    "Loan=" + Loan +
                    ", count=" + count +
                    '}';
        }

        public FloatWritable getLoan() {
            return Loan;
        }

        public IntWritable getCount() {
            return count;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "int rate count");
        job.setJarByClass(AverageInterestRateByGrade.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LoanAveragingPair.class);

        job.setMapperClass(MapLoan.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
