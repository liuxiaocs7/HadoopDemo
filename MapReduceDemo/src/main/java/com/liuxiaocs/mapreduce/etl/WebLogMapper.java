package com.liuxiaocs.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1.获取一行
        String line = value.toString();

        // 2.ETL
        boolean result = parseLog(line, context);
        
        if(!result) {
            return;
        }
        
        // 3.写出
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        // 切割
        String[] fields = line.split(" ");
        // 判断一下日志的长度是否大于11
        if(fields.length > 11) {
            return true;
        } else {
            return false;
        }
    }
}
