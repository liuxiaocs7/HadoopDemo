package com.liuxiaocs.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN: reduce阶段输入的key的类型(行号) IntWritable
 * VALUEIN: reduce阶段输入value的类型 Text
 * KEYOUT: reduce阶段输出的key类型 Text
 * VALUEOUT: reduce阶段输出的value类型 IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        // liuxiaocs (1, 1)
        // 累加
        for(IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key, outV);
    }
}
