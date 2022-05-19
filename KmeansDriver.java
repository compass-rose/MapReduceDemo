package com.atguigu.mapreduce.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KmeansDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length < 3) {
            throw new IllegalArgumentException("需要3个参数，储存centers数据的文件名，存储元数据的文件名，结果目录");
        }
        String centerPath = args[0];
        String dataPath = args[1];
        String newCenterPath = args[2];


//        centerPath = FileUtil.loadFile(newCenterPath, "MyKmeans", centerPath);
//        dataPath = FileUtil.loadFile(newCenterPath, "MyKmeans", dataPath);
//
//        FileUtil.deleteFile(newCenterPath);

        int count = 0;


        while (true) {
            run(centerPath, dataPath, newCenterPath, true);
            System.out.println(" 第 " + ++count + " 次计算 ");
            if (Utils.compareCenters(centerPath, newCenterPath)) {
                run(centerPath, dataPath, newCenterPath, false);
                break;
            }


        }


    }

    public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException, IOException {

        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);

        Job job = new Job(conf, "mykmeans");
        job.setJarByClass(KmeansDriver.class);

        job.setMapperClass(KmeansMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(runReduce){
            //最后依次输出不许要reduce
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
        }

        FileInputFormat.addInputPath(job, new Path(dataPath));

        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));

        System.out.println(job.waitForCompletion(true));
    }

}
