package com.atguigu.mapreduce.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class KmeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> value, Reducer<IntWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

        ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();

        //依次读取记录集，每行为一个ArrayList<Double>
        for(Iterator<Text> it = value.iterator(); it.hasNext();){
            ArrayList<Double> tempList = Utils.textToArray(it.next());
            filedsList.add(tempList);
        }

        //计算新的中心
        //每行的元素个数
        int filedSize = filedsList.get(0).size();
        double[] avg = new double[filedSize];
        for(int i=0;i<filedSize;i++){
            //求没列的平均值
            double sum = 0;
            int size = filedsList.size();
            for(int j=0;j<size;j++){
                sum += filedsList.get(j).get(i);
            }
            avg[i] = sum / size;
        }
        context.write(NullWritable.get() , new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));

    }

}
