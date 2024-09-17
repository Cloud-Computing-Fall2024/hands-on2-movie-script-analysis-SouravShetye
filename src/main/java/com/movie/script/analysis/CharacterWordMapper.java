package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // Convert the input line to a string and spliting by ':' to separate character and dialogue
        String line = value.toString();
        String[] parts = line.split(":", 2);

        // Ensuring there are both character and dialogue parts
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenizing the dialogue into words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);

            // For each word in the dialogue, writing a key-value pair to context
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toLowerCase();
                characterWord.set(characterName + ":" + token);
                context.write(characterWord, one);
            }
        }
    

    }
}
