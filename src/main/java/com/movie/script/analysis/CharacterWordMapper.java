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

        String line = value.toString();
        
        // Split by the first colon to get the character name and the dialogue
        String[] parts = line.split(":", 2);
        
        if (parts.length == 2) {
            String character = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue into words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase();  // Convert to lower case for uniformity
                characterWord.set(character + ":" + word);
                context.write(characterWord, one);
            }
        }
    }
}
