package com.atguigu.mapreduce.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    //中心集合
    ArrayList<ArrayList<Double>> centers = null;
    //用k个中心
    int k = 0;

    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        centers = Utils.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
        k = centers.size();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        //读取一行数据
        ArrayList<Double> fileds = Utils.textToArray(value);
        int sizeOfFileds = fileds.size();

        double minDistance = 99999999;
        int centerIndex = 0;

        //依次取出k个中心点与当前读取的记录做计算
        for(int i=0;i<k;i++){
            double currentDistance = 0;
            for(int j=0;j<sizeOfFileds;j++){
                double centerPoint = Math.abs(centers.get(i).get(j));
                double filed = Math.abs(fileds.get(j));
                currentDistance += Math.pow((centerPoint - filed) / (centerPoint + filed), 2);
            }
            //循环找出距离该记录最接近的中心点的ID
            if(currentDistance<minDistance){
                minDistance = currentDistance;
                centerIndex = i;
            }
        }
        //以中心点在centers中的索引为Key 将记录原样输出
        context.write(new IntWritable(centerIndex+1), value);
    }
}
