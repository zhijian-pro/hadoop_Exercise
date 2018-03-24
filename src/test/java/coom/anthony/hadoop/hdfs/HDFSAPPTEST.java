package coom.anthony.hadoop.hdfs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * @Description: hadoop hdfs java API测试
 * @Date: Created in 14:01 2018/3/24
 * @Author: Anthony_Duan
 */
public class HDFSAPPTEST {

    public static final String HDFS_PATH = "hdfs://localhost:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;


    /**
     * 创建文件夹
     * @throws Exception
     */
    @Test
    public void mkdir()throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{

        FSDataOutputStream ouput = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        ouput.write("hello hadoop".getBytes());
        ouput.flush();
        ouput.close();

    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream in  = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }



    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 从本地上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("/Users/duanjiaxing/myhexo/db.json");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }


    /**
     * 带进度条的上传
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress()throws Exception{
        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("/Users/duanjiaxing/software/kafka_2.11-0.9.0.0.tgz")
                )
        );
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/software"),
                new Progressable() {
                    public void progress() {
                        System.out.print(".");
                    }
                }
        );
        IOUtils.copyBytes(in,output,4096);
    }

    /**
     * 下载HDFS文件
     * @throws Exception
     */
    @Test
    public void copyTolocalFile()throws Exception{
        Path localPath = new Path("/Users/duanjiaxing/Desktop");
        Path hdfsPath = new Path("/hdfsapi/test/software/kafka_2.11-0.9.0.0.tgz");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    /**
     * 查看某个文件目录下所有文件
     * @throws Exception
     */
    @Test
    public void listFile() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses){
            String isDir = fileStatus.isDirectory()?"文件夹":"文件";
            short replication = fileStatus.getReplication();//文件的副本信息
            long len = fileStatus.getLen();//文件大小
            String path = fileStatus.getPath().toString();//全路径

            System.out.println(isDir+"\t"+replication+"\t"+len+"\t"+path);
        }
    }

    /**
     * 递归删除文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test/software/kafka_2.11-0.9.0.0.tgz"),true);
    }

    @Before
    public void setU() throws Exception{
        System.out.print("HDFSAPPtest.setUp");
        configuration = new Configuration();
//      这里可以传入用户名，我使用的是Mac本机上的hadoop，用户名是一样的，如果不是，需要传入用户名参数
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration);


    }

    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }

}
