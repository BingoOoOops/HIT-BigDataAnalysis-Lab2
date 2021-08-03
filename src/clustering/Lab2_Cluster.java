/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

package clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;


public class Lab2_Cluster {
    private static String inputPath = "hdfs://localhost:9000/user/lb/input/Lab2/";
    private static String outputPath = "hdfs://localhost:9000/user/lb/output/Lab2/";

    private static String iniCenterPath = inputPath + "config/initial_cluster_centers.txt";
    public static String inputCenterPath = inputPath + "MRinput/initial_cluster_centers.txt";

    //choose one of these 2 data file
    public static String dataPath = inputPath + "data/cluster_data_small.txt";
//    public static String dataPath = inputPath + "data/cluster_data.txt";

    private static String outputCenterPath = outputPath + "cluster/centers/";
    public static String resultFilePath = outputPath + "cluster/result/result.txt";

    private static int numOfClusters;


    /**
     * one round of MapReduce of clustering algorithm
     *
     * @param typeOfAlgorithm type of algorithm, need to be "K-Means" or "K-Medoids"
     * @param numOfRounds
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @SuppressWarnings("deprecation")
    private static void runAlgorithm(String typeOfAlgorithm, int numOfRounds)
            throws IOException, ClassNotFoundException, InterruptedException {

        numOfClusters = Tools.getDataFromHDFS(inputCenterPath, false).size();

        Configuration conf = new Configuration();
        conf.set("centersPath", inputCenterPath);
        conf.set("numOfClusters", String.valueOf(numOfClusters));
        conf.set("numOfRounds", String.valueOf(numOfRounds));

        Job job = new Job(conf, typeOfAlgorithm);
        job.setJarByClass(ClusterAlgorithm.class);

        job.setMapperClass(ClusterAlgorithm.Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(typeOfAlgorithm.equals("K-Means"))
        {
            job.setReducerClass(ClusterAlgorithm.k_Means_Reduce.class);
        }
        else {
            job.setReducerClass(ClusterAlgorithm.k_Medoids_Reduce.class);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(outputCenterPath));

        System.out.println(job.waitForCompletion(true));
    }



    /**
     * run clustering algorithm to get cluster centers.
     * K-Means and K-Medoids algorithms are included
     *
     * @param typeOfAlgorithm type of algorithm, need to be "K-Means" or "K-Medoids"
     * @return total number of MapReduce round
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static int calClusterCenters(String typeOfAlgorithm) throws InterruptedException, IOException, ClassNotFoundException {

        if(!(typeOfAlgorithm.equals("K-Means") || typeOfAlgorithm.equals("K-Medoids")))
        {
            throw new RuntimeException("argument need to be \"K-Means\" or \"K-Medoids\"");
        }

        int couRounds = 0;
        while (true) {
            couRounds++;
            System.out.println("\nRunning " + typeOfAlgorithm);
            System.out.println("Round: " + couRounds);
            System.out.println();

            ClusterAlgorithm.numOfTuples = 0;
            ClusterAlgorithm.clusterSizeInfo.clear();
            runAlgorithm(typeOfAlgorithm, couRounds);

            //cluster centers updated in the last round of MapReduce
            if (!Tools.AreCentersConverged(inputCenterPath, outputCenterPath)) {

                System.out.println("\n\nupdated centers\n\n");

                //clear inputCenterPath, the input location of MapReduce
                Configuration conf = new Configuration();
                Path MRinputPath = new Path(inputCenterPath);
                FileSystem fileSystem = MRinputPath.getFileSystem(conf);

                FSDataOutputStream overWrite = fileSystem.create(MRinputPath, true);
                overWrite.writeChars("");
                overWrite.close();

                //copy new center file in outputCenterPath to inputCenterPath as new cluster centers
                Path MRoutputPath = new Path(outputCenterPath);
                FileStatus[] listFiles = fileSystem.listStatus(MRoutputPath);
                for (FileStatus listFile : listFiles) {
                    FSDataOutputStream in = fileSystem.create(MRinputPath);
                    FSDataInputStream out = fileSystem.open(listFile.getPath());
                    IOUtils.copyBytes(out, in, 4096, true);
                }
                //clear outputCenterPath, the output location of MapReduce
                Tools.deletePath(outputCenterPath);
            }
            //cluster centers did not update in the last round of MapReduce.
            //Finalized cluster centers, which are store in inputCenterPath
            else {
                break;
            }
        }
        return couRounds;
    }


    /**
     * do some preparing work on HDFS before running MapReduce
     *
     * @throws IOException
     */
    private static void setUp() throws IOException {

        //delete output path if exist
        Path output = new Path(outputCenterPath);
        FileSystem hdfs = output.getFileSystem(new Configuration());
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        //initialize the starting center file, which may be modified in the last run of this program
        Path inputCenterFile = new Path(inputCenterPath);
        if (hdfs.exists(inputCenterFile)) {
            hdfs.delete(inputCenterFile, true);
        }

        OutputStream outputStream = hdfs.create(inputCenterFile);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));

        ArrayList<ArrayList<Double>> iniCenters = Tools.getDataFromHDFS(iniCenterPath, false);

        for(ArrayList<Double> itCenter : iniCenters)
        {
            bufferedWriter.write(itCenter.get(0).toString());
            for(int i = 1; i < itCenter.size(); i++)
            {
                bufferedWriter.write("," + itCenter.get(i).toString());
            }
            bufferedWriter.write("\n");
        }

        bufferedWriter.close();
        hdfs.close();
    }


    /**
     * summary of the result
     *
     * @param MapReduceRound
     */
    private static void summary(int MapReduceRound)
    {
        Tools.theMostImportant();
        System.out.println("-----Summary-----\n");
        System.out.println("MapReduceRound: " + MapReduceRound);
        System.out.println("Total point: " + ClusterAlgorithm.numOfTuples);
        System.out.println("Cluster number: " + numOfClusters);
        System.out.println("\nPoints in clusters: ");
        for(int i = 0 ; i < ClusterAlgorithm.clusterSizeInfo.size(); i++)
        {
            System.out.println("cluster " + i + ": " + ClusterAlgorithm.clusterSizeInfo.get(i) + " points");
        }
    }



    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        setUp();

//        int MapReduceRound = calClusterCenters("K-Means");
        int MapReduceRound = calClusterCenters("K-Medoids");    //highly discouraged on huge data set

        ClusterAlgorithm.assignTuplesToClusters();

        summary(MapReduceRound);
    }
}
