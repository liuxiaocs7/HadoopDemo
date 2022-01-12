package com.liuxiaocs.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    // Ctrl + Alt + F 升级为全局变量
    private FSDataOutputStream firstOut;
    private FSDataOutputStream secondOut;

    public LogRecordWriter(TaskAttemptContext job) {
        // 创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            firstOut = fs.create(new Path("D:\\Development\\Code\\IdeaProjects\\HadoopDemo\\output\\first.log"));
            secondOut = fs.create(new Path("D:\\Development\\Code\\IdeaProjects\\HadoopDemo\\output\\second.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 具体写
        String log = key.toString();
        if(log.contains("google")) {
            firstOut.writeBytes(log + "\n");
        } else {
            secondOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关闭流
        IOUtils.closeStream(firstOut);
        IOUtils.closeStream(secondOut);
    }
}
