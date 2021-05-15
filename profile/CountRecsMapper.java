import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String val = value.toString();
        String[] element = val.split(",");

        if (element[0].matches("[0-9]+")){
            word.set("Total number of records: ");
            context.write(word, one);
        }
    }
}