package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Iterate through all the values for this key and sum them up
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Set the sum as the result value
        result.set(sum);

        // Write the key and the aggregated count to the context
        context.write(key, result);
    }
}
