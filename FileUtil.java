package com.atguigu.mapreduce.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileUtil {
    public static String loadFile(String inputPath,String folder,String fileName) throws IOException{

        //获取数据文件的全路径


        if(null != folder && !"".equals(folder)){
            folder = folder + "/";
        }

        String srcPathDir = FileUtil.class.getProtectionDomain().getCodeSource().getLocation()
                .getFile() + folder + fileName;

        Path srcpath = new Path("file:///" + srcPathDir);

        Path dstPath = new Path(getJobRootPath(inputPath) + fileName);

        Configuration conf = new Configuration();

        FileSystem fs = dstPath.getFileSystem(conf);

        fs.delete(dstPath, true);

        fs.copyFromLocalFile(srcpath, dstPath);

        fs.close();

        return getJobRootPath(inputPath) + fileName;
    }

    /**
     * 如果路径的最后不包哈“/”就加一个“/”
     * @param path
     * @return
     */
    public static String getJobRootPath(String path){
        if(path.lastIndexOf("/") == path.length()-1){
            path = path.substring(0, path.lastIndexOf("/"));
        }
        return path.substring(0, path.lastIndexOf("/")+1);
    }

    public static void deleteFile(String ...filePath) throws IOException{
        Configuration conf = new Configuration();
        for (int i = 0; i < filePath.length; i++) {
            Path path = new Path(filePath[i]);
            FileSystem fs = path.getFileSystem(conf);
            fs.delete(path,true);
        }
    }

}
