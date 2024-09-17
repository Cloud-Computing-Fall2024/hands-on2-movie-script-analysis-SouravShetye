package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

     private IntWritable totalLength = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Iterating through all the lengths for the current character
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Setting the total length for the character
        totalLength.set(sum);

        // Writing the character and their total dialogue length to context
        context.write(key, totalLength);
    }
}
