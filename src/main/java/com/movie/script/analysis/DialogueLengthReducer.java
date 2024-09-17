package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalWordCount = 0;
        
        // Sum all the word counts for this character
        for (IntWritable val : values) {
            totalWordCount += val.get();
        }
        
        // Emit the total word count for the character
        context.write(key, new IntWritable(totalWordCount));
    }
}
