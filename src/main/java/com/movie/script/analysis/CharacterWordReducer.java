package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        // Sum up the counts for each word spoken by the character
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // Emit the word and its frequency
        context.write(key, new IntWritable(sum));
    }
}
