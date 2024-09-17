package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable dialogueLength = new IntWritable(); // Declare and initialize here
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Converting the input line to a string and split by ':' to separate character and dialogue
        String line = value.toString();
        String[] parts = line.split(":", 2);

        // Ensuring there are both character and dialogue parts
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Calculating the length of the dialogue (excluding leading/trailing spaces)
            int length = dialogue.length();

            // Setting the character's name as the key and the dialogue length as the value
            character.set(characterName);
            dialogueLength.set(length);

            // Writing the key-value pair to context
            context.write(character, dialogueLength);
        }
    }
}
