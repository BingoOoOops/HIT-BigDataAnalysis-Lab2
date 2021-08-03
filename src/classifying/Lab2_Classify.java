/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

package classifying;

import clustering.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Lab2_Classify {

    private static String hdfsInputPath = "hdfs://localhost:9000/user/lb/input/Lab2-classify";
    private static String hdfsOutputPath = "hdfs://localhost:9000/user/lb/output/Lab2-classify";

    private static String trainingDataInputPath = hdfsInputPath + "/training_data.txt";
    private static String verifyingDataInputPath = hdfsInputPath +"/verifying_data.txt";
    private static String testingDataInputPath = hdfsInputPath +"/testing_data.txt";

    private static String muOutputPath = hdfsOutputPath + "/mu/";
    private static String sigmaOutputPath = hdfsOutputPath + "/sigma/";

    private static String NaiveBayesTrainingDataOutputPath = hdfsOutputPath + "/NaiveBayes/training_data_result.txt";
    private static String NaiveBayesVerifyingDataOutputPath = hdfsOutputPath + "/NaiveBayes/verifying_data_result.txt";
    private static String NaiveBayesTestingDataOutputPath = hdfsOutputPath + "/NaiveBayes/testing_data_result.txt";

    private static String SimplifiedMahalanobisDistanceTrainingDataOutputPath = hdfsOutputPath + "/Distance/training_data_result.txt";
    private static String SimplifiedMahalanobisDistanceVerifyingDataOutputPath = hdfsOutputPath + "/Distance/verifying_data_result.txt";
    private static String SimplifiedMahalanobisDistanceTestingDataOutputPath = hdfsOutputPath + "/Distance/testing_data_result.txt";


    /**
     * calculate mu of each feature in one round of MapReduce
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private static void calculateMuOfFeatures() throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "NaiveBayes_calculate_mu");
        job.setJarByClass(ClassifyAlgorithm.class);

        job.setMapperClass(ClassifyAlgorithm.CalculateMuMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ClassifyAlgorithm.CalculateMuReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(trainingDataInputPath));
        FileOutputFormat.setOutputPath(job, new Path(muOutputPath));

        System.out.println(job.waitForCompletion(true));
    }


    /**
     * calculate sigma of each feature in one round of MapReduce
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private static void calculateSigmaOfFeatures() throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "NaiveBayes_calculate_sigma");
        job.setJarByClass(ClassifyAlgorithm.class);

        job.setMapperClass(ClassifyAlgorithm.CalculateSigmaMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ClassifyAlgorithm.CalculateSigmaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(trainingDataInputPath));
        FileOutputFormat.setOutputPath(job, new Path(sigmaOutputPath));

        System.out.println(job.waitForCompletion(true));
    }


    /**
     * get mu and sigma of the classifier
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static void trainModel() throws InterruptedException, IOException, ClassNotFoundException {

        Tools.deletePathIfExist(muOutputPath);
        Tools.deletePathIfExist(sigmaOutputPath);

        calculateMuOfFeatures();
        calculateSigmaOfFeatures();
        ClassifyAlgorithm.calculateProbability();
    }


    /**
     * summary of the result
     *
     * @param accuracyList accuracy of every test
     * @param algorithmType type of classification algorithm
     */
    private static void summary(List<String> accuracyList, String algorithmType)
    {
        Tools.theMostImportant();
        System.out.println("-----Summary-----");
        System.out.println("\n\npoints number information");
        for(String itClass : ClassifyAlgorithm.pointsNumOfFeatures.keySet())
        {
            System.out.println("Class " + itClass + "    " + ClassifyAlgorithm.pointsNumOfFeatures.get(itClass));
        }

        System.out.println("\n\naverage of features");
        for(String itClass : ClassifyAlgorithm.muOfFeatures.keySet())
        {
            System.out.println("Class " + itClass + "    " + Tools.doubleArrayToString(ClassifyAlgorithm.muOfFeatures.get(itClass)));
        }

        System.out.println("\n\nstandard deviation of features");
        for(String itClass : ClassifyAlgorithm.sigmaOfFeatures.keySet())
        {
            System.out.println("Class " + itClass + "    " + Tools.doubleArrayToString(ClassifyAlgorithm.sigmaOfFeatures.get(itClass)));
        }

        System.out.println("\n\n" + algorithmType);
        System.out.println("accuracy of the model being applied to test data set");
        for(int i = 0; i < accuracyList.size(); i++)
        {
            System.out.println("Test " + i + ": " + accuracyList.get(i));
        }
    }

    /**
     * classify data points using a type of algorithm
     *
     * @param algorithmType type of classification algorithm
     * @return
     * @throws IOException
     */
    private static List<String> classifyAndTest(String algorithmType) throws IOException {

        String trainingOutput;
        String verifyingOutput;
        String testingOutput;
        if(algorithmType.equals("Naive Bayes"))
        {
            trainingOutput = NaiveBayesTrainingDataOutputPath;
            verifyingOutput = NaiveBayesVerifyingDataOutputPath;
            testingOutput = NaiveBayesTestingDataOutputPath;
        }
        else {
            trainingOutput = SimplifiedMahalanobisDistanceTrainingDataOutputPath;
            verifyingOutput = SimplifiedMahalanobisDistanceVerifyingDataOutputPath;
            testingOutput = SimplifiedMahalanobisDistanceTestingDataOutputPath;
        }

        List<String> accuracyList = new ArrayList<>();
        accuracyList.add(ClassifyAlgorithm.classifyPoints(algorithmType, trainingDataInputPath, trainingOutput));
        accuracyList.add(ClassifyAlgorithm.classifyPoints(algorithmType, verifyingDataInputPath, verifyingOutput));
        ClassifyAlgorithm.classifyPoints(algorithmType, testingDataInputPath, testingOutput);

        return accuracyList;
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {


        String algorithmType = "Naive Bayes";
//        String algorithmType = "Simplified Mahalanobis Distance";

        trainModel();

        List<String> accuracyList = classifyAndTest(algorithmType);

        summary(accuracyList, algorithmType);

    }
}
