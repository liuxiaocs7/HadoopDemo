package com.liuxiaocs.mapreduce.combineTextInputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: map阶段输入的key的类型(行号) LongWritable
 * VALUEIN: map阶段输入value的类型 Text
 * KEYOUT: map阶段输出的key类型 Text
 * VALUEOUT: map阶段输出的value类型 IntWritable
 *
 * 按行进行处理，只需要编写每一行的程序即可
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static Text outK = new Text();
    private static IntWritable outV = new IntWritable(1);

    /**
     * LongWritable key, Text value表示输入数据中的类型
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1.获取1行
        // liuxiaocs liuxiaocs
        // 将Text转化为String类型操作
        String line = value.toString();

        // 2.切割
        // liuxiaocs
        // liuxiaocs
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word : words) {
            // 封装outK
            outK.set(word);
            // 写出数据
            context.write(outK, outV);
        }
    }
}
