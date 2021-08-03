/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

package classifying;

import clustering.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.*;

public class ClassifyAlgorithm {

    /**
     * a map of [class, number of points in the class]
     */
    public static Map<String, Integer> pointsNumOfFeatures = new HashMap<>();
    /**
     * a map of [class, occurrence probability of the class]
     */
    public static Map<String, Double> probabilityOfClass = new HashMap<>();
    /**
     * a map of [class, average of each feature of the class]
     */
    public static Map<String, List<Double>> muOfFeatures = new HashMap<>();
    /**
     * a map of [class, standard deviation of each feature of the class]
     */
    public static Map<String, List<Double>> sigmaOfFeatures = new HashMap<>();


    /**
     * get the class of current data point
     *
     * @param point the point with class information at the end, separated by " "
     * @return the class of current data point
     */
    private static String getClassBelongsTo(Text point)
    {
        ArrayList<String> fieldsString = Tools.textToStringArray(point);
        return fieldsString.get(fieldsString.size() - 1);
    }


    /**
     * get features of input point, the last dimension is not coordinate data but the class the point belongs to
     *
     * @param point a line of point
     * @return features of the point
     */
    private static ArrayList<Double> getFields(Text point)
    {
        ArrayList<Double> fields = Tools.textToDoubleArray(point);
        fields.remove(fields.size() - 1);
        return fields;
    }


    /**
     * calculate average features of point in points.
     * NOTE THAT this function will put <classBelongTo, number of points> into NaiveBayes.pointsNumOfFeatures implicitly
     *
     * @param points points to calculate features's average value
     * @param classBelongTo the class these input points belong to, put "null" if there is no need to update NaiveBayes.pointsNumOfFeatures
     * @return average list of features of points
     */
    private static List<Double> getAvgOfFeatures(@NotNull Iterable<Text> points, String classBelongTo) {

        int numOfPoints = 0;
        List<Double> sumFeatures = new ArrayList<>();
        for(Text itPoint : points)
        {
            numOfPoints++;
            ArrayList<Double> itFeatures = Tools.textToDoubleArray(itPoint);
            if(numOfPoints == 1)
            {
                sumFeatures = itFeatures;
            }
            else {
                for(int i = 0; i < itFeatures.size(); i++)
                {
                    sumFeatures.set(i, sumFeatures.get(i) + itFeatures.get(i));
                }
            }
        }
        if(!classBelongTo.equals("null"))
        {
            pointsNumOfFeatures.put(classBelongTo, numOfPoints);
        }
        List<Double> avgFeatures = new ArrayList<>();
        for(Double itFeature : sumFeatures)
        {
            avgFeatures.add(itFeature / numOfPoints);
        }
        return avgFeatures;
    }


    /**
     * calculate occurrence probability of each class
     */
    public static void calculateProbability()
    {
        Integer allPointsNum = 0;
        for(int it : pointsNumOfFeatures.values()){
            allPointsNum += it;
        }
        for(String itClass : pointsNumOfFeatures.keySet())
        {
            Integer pointNumInClass = pointsNumOfFeatures.get(itClass);
            probabilityOfClass.put(itClass, pointNumInClass.doubleValue() / allPointsNum.doubleValue());
        }
    }


    /**
     * classify points to different classes using certain algorithm. Calculate the classification accuracy if input
     * data points have certain class belong to
     *
     * @param algorithmType the algorithm type to use
     * @param inputPathString HDFS path of input data points
     * @param outputPathString output path to store result file
     * @return accuracy of classification, or String "undefined" if input data points do not belong to certain class
     * @throws IOException
     */
    static String classifyPoints(String algorithmType, String inputPathString, String outputPathString)
            throws IOException {

        Path outputPath = new Path(outputPathString);

        FileSystem hdfs = outputPath.getFileSystem(new Configuration());
        OutputStream outputStream = hdfs.create(outputPath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

        ArrayList<ArrayList<Double>> inputPoints = Tools.getDataFromHDFS(inputPathString, false);
        boolean needToTest = false;

        //input file is a testing or verifying file with defined class a point belongs to at the end of a line
        if(inputPoints.get(0).size() != muOfFeatures.values().iterator().next().size())
        {
            needToTest = true;
        }
        String accuracy = "undefined";
        Integer couAll = 0;
        Integer couRight = 0;

        for(ArrayList<Double> itPoint : inputPoints)
        {
            couAll++;
//
//            if(!needToTest)
//            {
//                StringBuilder stringBuilder = new StringBuilder();
//                for(Double it : itPoint)
//                {
//                    stringBuilder.append(it).append(", ");
//                }
//                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
//                System.out.println("\n\n[Testing]  Point: " + stringBuilder);
//                System.out.println();
//            }

            String classBelongsTo;
            if(algorithmType.equals("Naive Bayes"))
            {
                classBelongsTo = classifyUsingNaiveBayes(itPoint);
            }
            else if(algorithmType.equals("Simplified Mahalanobis Distance"))
            {
                classBelongsTo = classifyUsingSimplifiedMahalanobisDistance(itPoint);
            }
            else {
                throw new RuntimeException("wrong input algorithm type, " +
                        "need to be \"Naive Bayes\" or \"Simplified Mahalanobis Distance\"");
            }
            bufferedWriter.write(classBelongsTo + "\n");


            if(!needToTest)
            {
                System.out.println("[" + algorithmType + "]  classify result: " + classBelongsTo);
            }

            //test result
            if(needToTest)
            {
                NumberFormat num = NumberFormat.getPercentInstance();
                num.setMinimumFractionDigits(2);
                if(itPoint.get(itPoint.size() - 1).equals(Double.parseDouble(classBelongsTo)))
                {
                    couRight++;
                    accuracy = num.format(couRight.doubleValue() / couAll.doubleValue());
                    System.out.println("[Testing  " + algorithmType + "]  correct              Accuracy: " + accuracy);
                }
                else {
                    accuracy = num.format(couRight.doubleValue() / couAll.doubleValue());
                    System.out.println("[Testing  " + algorithmType + "]           wrong...    Accuracy: " + accuracy);
                }
            }
        }
        bufferedWriter.close();
        hdfs.close();
        return accuracy;
    }


    /**
     * use naive Bayes algorithm to classify the point to a class
     *
     * @param point the point to classify
     * @return classification result
     */
    private static String classifyUsingNaiveBayes(ArrayList<Double> point)
    {
        //calculate likelihood of every class and choose the one with the maximum likelihood as the result
        String classBelongsTo = "null";
        double maxLikelihood = Double.MIN_VALUE;

        for(String itClass : muOfFeatures.keySet())
        {
            Double itProbability = probabilityOfClass.get(itClass);
            List<Double> itMu = muOfFeatures.get(itClass);
            List<Double> itSigma = sigmaOfFeatures.get(itClass);

            //in naive Bayes, likelihood of every feature are simply multiplied as the whole likelihood
            Double likeLiHoodOfTheClass = itProbability;
            for(int i = 0; i < itMu.size(); i++)
            {
                Double likelihoodOfTheFeature = getGaussianValue(point.get(i), itMu.get(i), itSigma.get(i));
                likeLiHoodOfTheClass *= likelihoodOfTheFeature;
            }
            if(likeLiHoodOfTheClass > maxLikelihood)
            {
                maxLikelihood = likeLiHoodOfTheClass;
                classBelongsTo = itClass;
            }
        }
        return classBelongsTo;
    }



    /**
     * use simplified Mahalanobis distance algorithm to classify the point to a class
     *
     * @param point the point to classify
     * @return classification result
     */
    private static String classifyUsingSimplifiedMahalanobisDistance(ArrayList<Double> point) {

        String classBelongsTo = "null";
        double minDistance = Double.MAX_VALUE;
        for(String itClass : muOfFeatures.keySet())
        {
            List<Double> itMu = muOfFeatures.get(itClass);
            List<Double> itSigma = sigmaOfFeatures.get(itClass);

            Double distanceOfTheClass = 0.0;
            for(int i = 0; i < itMu.size(); i++)
            {
                distanceOfTheClass +=  Math.pow(point.get(i) - itMu.get(i), 2) / itSigma.get(i);
            }
            if(distanceOfTheClass < minDistance)
            {
                minDistance = distanceOfTheClass;
                classBelongsTo = itClass;
            }
        }
        return classBelongsTo;
    }



    /**
     * get Gaussian value of x due to mu and sigma
     *
     * @param x input value
     * @param mu average
     * @param sigma standard deviation
     * @return Gaussian value of x
     */
    private static Double getGaussianValue(Double x, Double mu, Double sigma) {
        if(sigma == 0)
        {
            sigma = 1e-10;
        }
        return (1.0 / (sigma * Math.sqrt(2.0 * Math.PI))) * Math.pow(Math.E, -Math.pow(x - mu, 2) / (2 * Math.pow(sigma, 2)));
    }


    public static class CalculateMuMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * classify points by the class belong to.
         *
         * @param key default input key
         * @param value the coordinate of the point, with the class it belongs to at the end
         * @param context <class the point belongs to, features of the point>
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String classBelongsTo = getClassBelongsTo(value);
            ArrayList<Double> fields = getFields(value);

            System.out.println("[Training: calculating mu]  input class: " + classBelongsTo);
//            System.out.println("[Training]  input fields: " + Tools.doubleArrayToString(fields));


            context.write(new Text(classBelongsTo), Tools.doubleArrayToText(fields));
        }
    }


    public static class CalculateMuReducer extends Reducer<Text, Text, Text, Text> {


        /**
         * calculate the average of all features and the number of points of each class.
         *
         * @param key the class of these points belong to
         * @param points points in the same class
         * @param context <class, average features of the class>
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<Text> points, Context context)
                throws IOException, InterruptedException {

            String classBelongsTo = key.toString();

            muOfFeatures.put(classBelongsTo, getAvgOfFeatures(points, classBelongsTo));

            String muString = Tools.doubleArrayToString(muOfFeatures.get(classBelongsTo));

            System.out.println("\n");
            System.out.println("[Training]  class: " + classBelongsTo);
            System.out.println("[Training]  mu of features: " + muString);
            System.out.println();

            context.write(new Text(classBelongsTo), new Text(muString));
        }
    }


    public static class CalculateSigmaMapper extends Mapper<LongWritable, Text, Text, Text> {


        /**
         * calculate Square Error of every point to the class it belongs to.
         *
         * @param key default input key
         * @param value the coordinate of the point
         * @param context <class the point belongs to, Square Error of every features of the point>
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String classBelongsTo = getClassBelongsTo(value);
            ArrayList<Double> fields = getFields(value);
            List<Double> avgList = muOfFeatures.get(classBelongsTo);
            ArrayList<Double> squareErrorList = new ArrayList<>();

            for(int i = 0; i < fields.size(); i++)
            {
                squareErrorList.add(Math.pow(fields.get(i) - avgList.get(i), 2));
            }
            System.out.println("[Training: calculating sigma]  input class: " + classBelongsTo);
            context.write(new Text(classBelongsTo), Tools.doubleArrayToText(squareErrorList));
        }
    }


    public static class CalculateSigmaReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * calculate the standard deviation of all features of each class.
         *
         * @param key the class of these points belong to
         * @param squareErrorLists list of Square Error of every points in the class
         * @param context <<class, standard deviation of every features of the class>
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<Text> squareErrorLists, Context context) throws IOException, InterruptedException {

            String classBelongsTo = key.toString();
            List<Double> varianceList = getAvgOfFeatures(squareErrorLists, "null");
            List<Double> sigmaList = new ArrayList<>();

            for(Double itVariance : varianceList)
            {
                sigmaList.add(Math.sqrt(itVariance));
            }
            sigmaOfFeatures.put(classBelongsTo, sigmaList);

            String sigmaString = Tools.doubleArrayToString(sigmaOfFeatures.get(classBelongsTo));

            System.out.println("\n");
            System.out.println("[Training]  class: " + classBelongsTo);
            System.out.println("[Training]  sigma of features: " + sigmaString);
            System.out.println();

            context.write(new Text(classBelongsTo), new Text(sigmaString));
        }
    }
}