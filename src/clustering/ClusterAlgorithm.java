/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

package clustering;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ClusterAlgorithm {

    //don't put this field into class Map, otherwise it will restart counting tuples from 0 due to Hadoop's feature
    //also, please reset numOfTuples and clusterSizeInfo before every round of MapReduce outside this class
    public static int numOfTuples = 0;
    public static List<Integer> clusterSizeInfo = new ArrayList<>();



    /**
     * assign every tuple in dataPath to a cluster whose center is stored in inputCenterPath and store the clustering result
     * in resultFilePath
     *
     * @throws IOException
     */
    static void assignTuplesToClusters() throws IOException {

        //assign every tuple to the final cluster centers
        Path resultFile = new Path(Lab2_Cluster.resultFilePath);
        FileSystem hdfs = resultFile.getFileSystem(new Configuration());
        if (hdfs.exists(resultFile)) {
            hdfs.delete(resultFile, true);
        }
        OutputStream outputStream = hdfs.create(resultFile);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));

        ArrayList<ArrayList<Double>> centers = Tools.getDataFromHDFS(Lab2_Cluster.inputCenterPath, false);
        ArrayList<ArrayList<Double>> tuples = Tools.getDataFromHDFS(Lab2_Cluster.dataPath, false);

        for(ArrayList<Double> iterTuple : tuples)
        {
            double minDistance = Double.MAX_VALUE;
            int clusterNumBelongsTO = -1;

            for(int i = 0; i < centers.size(); i++)
            {
                double currentDistance = Tools.calculateDistanceOf2Point(iterTuple, centers.get(i));
                if(currentDistance < minDistance)
                {
                    minDistance = currentDistance;
                    clusterNumBelongsTO = i;
                }
            }
            bufferedWriter.write(clusterNumBelongsTO + "\n");
        }

        bufferedWriter.close();
        hdfs.close();

    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {


        private ArrayList<ArrayList<Double>> centers;
        private int numOfClusters;

        //getting input cluster centers
        protected void setup(Context context) throws IOException {
            centers = Tools.getDataFromHDFS(context.getConfiguration().get("centersPath"), false);
            numOfClusters = Integer.parseInt(context.getConfiguration().get("numOfClusters"));
        }


        /**
         * update the nearest cluster tuple belongs to.
         * output format: <cluster number tuple belongs to, tuple>
         *
         * @param key
         * @param value tuple value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //getting a tuple
            ArrayList<Double> tuple = Tools.textToDoubleArray(value);

            numOfTuples++;
            System.out.println("numOfTuples: " + numOfTuples);

            double minDistance = Double.MAX_VALUE;
            int clusterNumBelongsTO = -1;

            //finding the nearest cluster current tuple belongs to
            for (int i = 0; i < numOfClusters; i++) {
                double currentDistance = Tools.calculateDistanceOf2Point(tuple, centers.get(i));

                if (currentDistance < minDistance) {
                    minDistance = currentDistance;
                    clusterNumBelongsTO = i;
                }
            }
            //cluster number as the key, tuple as the value
            context.write(new IntWritable(clusterNumBelongsTO), value);
        }
    }

    public static class k_Means_Reduce extends Reducer<IntWritable, Text, Text, NullWritable> {

        /**
         * update message of cluster centers.
         *
         *
         * @param key cluster number
         * @param value tuples in this cluster
         * @param context new cluster centers
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            ArrayList<ArrayList<Double>> tuples = new ArrayList<>();

            //getting all tuples in current cluster
            for (Text aValue : value) {
                ArrayList<Double> tempList = Tools.textToDoubleArray(aValue);
                tuples.add(tempList);
            }

            //calculate new center of current cluster
            int tupleSize = tuples.size();
            clusterSizeInfo.add(tupleSize);
            int fieldSize = tuples.get(0).size();
            double[] newCenter = new double[fieldSize];

            for (int i = 0; i < fieldSize; i++) {
                double sum = 0;
                for (ArrayList<Double> tuple : tuples) {
                    sum += tuple.get(i);
                }
                newCenter[i] = sum / tupleSize;
            }
            //output new cluster centers
            context.write(new Text(Arrays.toString(newCenter).replace("[", "").replace("]", "")), NullWritable.get());
        }
    }



    public static class k_Medoids_Reduce extends Reducer<IntWritable, Text, Text, NullWritable> {

        private int numOfClusters;
        private int processedTuple = 0;


        /**
         * update message of cluster centers.
         *
         *
         * @param key cluster number
         * @param value tuples in this cluster
         * @param context new cluster centers
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {

            ArrayList<ArrayList<Double>> tuples = new ArrayList<>();

            //getting all tuples in current cluster
            for (Text aValue : value) {
                ArrayList<Double> tempList = Tools.textToDoubleArray(aValue);
                tuples.add(tempList);
            }

            int tupleSize = tuples.size();
            clusterSizeInfo.add(tupleSize);
            numOfClusters = Integer.parseInt(context.getConfiguration().get("numOfClusters"));

            //choose a point as the new center of current cluster
            double minDistance = Double.MAX_VALUE;
            ArrayList<Double> newCenter = new ArrayList<>();
            int cou = 0;

            for(ArrayList<Double> iterCenter : tuples)
            {
                cou++;
                processedTuple++;
                NumberFormat num = NumberFormat.getPercentInstance();
                num.setMinimumFractionDigits(2);

                System.out.println("MapReduce round: " + context.getConfiguration().get("numOfRounds"));
                System.out.println("Progress of this round: " + num.format(Double.valueOf(processedTuple) / Double.valueOf(numOfTuples)));  //ignore the "Unnecessary boxing" warning here, of course it's necessary...

                System.out.println("Cluster number: " + clusterSizeInfo.size() + "rd(st/nd) of all " + numOfClusters + " clusters");
                System.out.println("Point number: " + cou + "rd(st/nd) of all " + tupleSize + " points");
                System.out.println("Be patient...I'm working really really hard to see if it can be the new center...");
                System.out.println(".\n.\n.\n\n");


                double costSum = 0.0;
                for(ArrayList<Double> iterPoint : tuples)
                {
                    costSum += Tools.calculateDistanceOf2Point(iterCenter, iterPoint);
                }

                if(costSum < minDistance)
                {
                    minDistance = costSum;
                    newCenter = iterCenter;
                }
            }

            StringBuilder newCenterSB = new StringBuilder();
            for(double iterField : newCenter)
            {
                newCenterSB.append(iterField).append(",");
            }
            newCenterSB.deleteCharAt(newCenterSB.length() - 1);

            context.write(new Text(newCenterSB.toString()), NullWritable.get());
        }
    }
}
