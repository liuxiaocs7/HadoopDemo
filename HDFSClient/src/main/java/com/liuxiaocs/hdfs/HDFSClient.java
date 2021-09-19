package com.liuxiaocs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS和Zookeeper都是以这种思路进行操作
 */
public class HDFSClient {

    // Ctrl + Alt + F升级为全局变量
    private FileSystem fs;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        // 设置副本数量为2
        configuration.set("dfs.replication", "2");
        // 用户
        String user = "liuxiaocs";

        // 1.获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3.关闭资源
        fs.close();
    }

    /**
     * 测试创建目录
     */
    @Test
    public void testmkdir() throws IOException {
        // 2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**
     * 上传文件
     * 参数优先级：hdfs-default.xml(优先级最低) => hdfs-site.xml => 在项目资源目录下的配置文件 => 代码里面的配置(优先级最高)
     */
    @Test
    public void testPut() throws IOException {
        // 参数解读
        // 参数一：表示是否删除本机上的原数据
        // 参数二：是否允许覆盖HDFS中的文件数据
        // 参数三：原数据路径
        // 参数四：目的地路径：/xiyou/huaguoshan 也可以写为 hdfs://hadoop102/xiyou/huaguoshan
        fs.copyFromLocalFile(
                false,
                true,
                new Path("D:\\Development\\Code\\IdeaProjects\\HadoopDemo\\HDFSClient\\src\\main\\resources\\testdata\\sunwukong.txt"),
                new Path("/xiyou/huaguoshan")
        );
    }

    /**
     * 文件下载
     */
    @Test
    public void testGet() throws IOException {
        // 参数解读
        // 参数一：原文件是否删除(HDFS中的文件)
        // 参数二：原文件路径HDFS
        // 参数三：目标地址路径Win(本地路径，即下载下来的文件存储的位置)
        // 参数四：是否开启校验，false开启校验，true不开启校验
        fs.copyToLocalFile(
                false,
                new Path("hdfs://hadoop102/xiyou/huaguoshan/sunwukong.txt"),
                new Path("D:\\Development\\Code\\IdeaProjects\\HadoopDemo\\HDFSClient\\src\\main\\resources\\testdata\\sunwukong1.txt"),
                false
        );
    }

    /**
     * 删除
     */
    @Test
    public void testRm() throws IOException {
        // 删除文件
        // 参数解读
        // 参数1: 要删除的路径(HDFS中的路径)
        // 参数2: 是否递归删除
        fs.delete(new Path("/sunwukong.txt"), false);

        // 删除空目录
        fs.delete(new Path("/xiyou"), false);

        // 删除非空目录(需要递归删除)
        fs.delete(new Path("/jinguo"), true);
    }

    /**
     * 移动和重命名
     */
    @Test
    public void testmv() throws IOException {
        // 文件重命名
        // 参数解读
        // 参数1: 原文件路径
        // 参数2: 目标文件路径
        fs.rename(new Path("/input/word.txt"), new Path("/input/ss.txt"));

        // 文件的移动和更名
        fs.rename(new Path("/input/ss.txt"), new Path("/cls.txt"));

        // 目录更名
        fs.rename(new Path("/input"), new Path("/input_rename"));
    }

    /**
     * 获取文件详细信息
     */
    @Test
    public void fileDetail() throws IOException {
        // 获取所有文件信息
        // 参数解读
        // 参数1: 表示原数据路径
        // 参数2: 表示是否递归获取所有文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 遍历文件
        while(listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("==========" + fileStatus.getPath() + "==========");
            System.out.println("操作权限: " + fileStatus.getPermission());
            System.out.println("所有者: " + fileStatus.getOwner());
            System.out.println("所在组: " + fileStatus.getGroup());
            System.out.println("文件长度: " + fileStatus.getLen());
            System.out.println("修改时间: " + fileStatus.getModificationTime());
            System.out.println("备份数量: " + fileStatus.getReplication());
            System.out.println("块大小: " + fileStatus.getBlockSize());
            System.out.println("文件名: " + fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
            System.out.println();
        }
    }

    /**
     * 判断是文件还是目录
     */
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件: " + status.getPath().getName());
            } else {
                System.out.println("目录: " + status.getPath().getName());
            }
        }
    }
}
