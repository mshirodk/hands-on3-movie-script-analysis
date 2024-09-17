package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        
        // Split by the first colon to separate the character and dialogue
        String[] parts = line.split(":", 2);
        
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Count the number of words in the dialogue
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            int wordCountForDialogue = tokenizer.countTokens();

            // Set the word count and character
            wordCount.set(wordCountForDialogue);
            character.set(characterName);

            // Emit (characterName, wordCount)
            context.write(character, wordCount);
        }
    }
}
