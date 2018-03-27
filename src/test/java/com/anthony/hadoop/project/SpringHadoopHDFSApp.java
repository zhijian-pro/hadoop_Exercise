package com.anthony.hadoop.project;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * @Description:
 * @Date: Created in 12:11 2018/3/27
 * @Author: Anthony_Duan
 */
public class SpringHadoopHDFSApp {

    private ApplicationContext ctx;

    private FileSystem fileSystem;


    /**
     * 本地上传到hdfs
     * @throws IOException
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path localPath = new Path("/Users/duanjiaxing/data/imooc/wc.txt");
        Path hdfsPath = new Path("/springhdfs");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 创建HDFS文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/springhdfs"));
    }




    @Before
    public void setUp(){

        ctx = new ClassPathXmlApplicationContext("beans.xml");

        fileSystem = (FileSystem)ctx.getBean("fileSystem");
    }

    @After
    public void tearDown(){
        ctx=null;
    }
}
