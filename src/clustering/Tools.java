/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

package clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Tools {


    /**
     * the most important thing I want to tell you my fellow
     */
    public static void theMostImportant()
    {
        System.out.println("\n\n\n");
        System.out.println(
                "      ⣿⣿⣿⣿⣿⠟⠋⠄⠄⠄⠄⠄⠄⠄⢁⠈⢻⢿⣿⣿⣿⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣿⠃⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠈⡀⠭⢿⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⡟⠄⢀⣾⣿⣿⣿⣷⣶⣿⣷⣶⣶⡆⠄⠄⠄⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⡇⢀⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⠄⠄⢸⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣇⣼⣿⣿⠿⠶⠙⣿⡟⠡⣴⣿⣽⣿⣧⠄⢸⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣿⣾⣿⣿⣟⣭⣾⣿⣷⣶⣶⣴⣶⣿⣿⢄⣿⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣿⣿⣿⣿⡟⣩⣿⣿⣿⡏⢻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣿⣿⣹⡋⠘⠷⣦⣀⣠⡶⠁⠈⠁⠄⣿⣿⣿⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣿⣿⣍⠃⣴⣶⡔⠒⠄⣠⢀⠄⠄⠄⡨⣿⣿⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⣿⣿⣿⣦⡘⠿⣷⣿⠿⠟⠃⠄⠄⣠⡇⠈⠻⣿⣿⣿⣿\n" +
                "      ⣿⣿⣿⣿⡿⠟⠋⢁⣷⣠⠄⠄⠄⠄⣀⣠⣾⡟⠄⠄⠄⠄⠉⠙⠻\n" +
                "      ⡿⠟⠋⠁⠄⠄⠄⢸⣿⣿⡯⢓⣴⣾⣿⣿⡟⠄⠄⠄⠄⠄⠄⠄⠄\n" +
                "      ⠄⠄⠄⠄⠄⠄⠄⣿⡟⣷⠄⠹⣿⣿⣿⡿⠁⠄⠄⠄⠄⠄⠄⠄⠄\n" +
                "\n" +
                "prosperity  democracy   civility   harmony\n" +
                "freedom     equality    justice    rule of law\n" +
                "patriotism  dedication  integrity  friendship\n" +
                "\n" +
                "            心中有党！    成绩理想！         " +
                "\n\n\n"
        );
    }


    /**
     * get data content from dataPath
     *
     * @param dataPath data path
     * @param isDirectory dataPath is directory or not
     * @return an ArrayList holding each line of ArrayList<Double> format
     * @throws IOException
     */
    public static ArrayList<ArrayList<Double>> getDataFromHDFS(String dataPath, boolean isDirectory) throws IOException {

        ArrayList<ArrayList<Double>> result = new ArrayList<>();

        Configuration conf = new Configuration();
        Path path = new Path(dataPath);
        FileSystem fileSystem = path.getFileSystem(conf);

        if (isDirectory) {
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (FileStatus aListFile : listFile) {
                result.addAll(getDataFromHDFS(aListFile.getPath().toString(), false));
            }
            return result;
        }

        FSDataInputStream fsd = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsd, conf);

        Text line = new Text();

        while (lineReader.readLine(line) > 0) {
            ArrayList<Double> tempList = textToDoubleArray(line);
            result.add(tempList);
        }
        lineReader.close();
        return result;
    }

    /**
     * delete file from HDFS path
     *
     * @param pathStr the path in HDFS
     * @throws IOException
     */
    public static void deletePath(String pathStr) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path, true);
    }

    /**
     * split Text by "," or "\t" to ArrayList of Double
     *
     * @param text the text to convert
     * @return content in ArrayList<Double> format
     */
    public static ArrayList<Double> textToDoubleArray(Text text) {

        ArrayList<Double> listDouble = new ArrayList<>();
        ArrayList<String> textStringList = Tools.textToStringArray(text);
        for (String field : textStringList) {
            listDouble.add(Double.parseDouble(field));
        }
        return listDouble;
    }


    /**
     * split Text by "," or "\t" into ArrayList of String
     *
     * @param text the text to convert
     * @return content in ArrayList<String> format
     */
    public static ArrayList<String> textToStringArray(Text text)
    {
        String[] fields = text.toString().split("[,\\t]");
        return new ArrayList<>(Arrays.asList(fields));
    }


    /**
     * convert ArrayList of Double into Text, separate by ","
     *
     * @param list ArrayList to convert
     * @return content in Text format
     */
    public static Text doubleArrayToText(List<Double> list) {
        StringBuilder stringBuilder = new StringBuilder();
        for(Double it : list)
        {
            stringBuilder.append(it).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return new Text(stringBuilder.toString());
    }


    /**
     * convert ArrayList of Double into String, separate by ","
     *
     * @param list ArrayList to convert
     * @return content in String format
     */
    public static String doubleArrayToString(List<Double> list) {

        StringBuilder stringBuilder = new StringBuilder();
        for(Double it : list)
        {
            stringBuilder.append(it.toString()).append(", ");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    /**
     * calculate the distance of p1 and p2.
     * p1 and p2 MUST have the same size.
     *
     * @param p1 point 1
     * @param p2 point 2
     * @return the distance
     */
    public static double calculateDistanceOf2Point(ArrayList<Double> p1, ArrayList<Double> p2)
    {
        double distance = 0.0;
        for(int i = 0; i < p1.size(); i++)
        {
            distance += Math.pow(p1.get(i) - p2.get(i), 2);
        }
        return distance;
    }

    /**
     * test the difference of 2 groups of clustering centers
     *
     * @param oldCenterPath path of old clustering centers
     * @param newCenterPath path of new clustering centers
     * @return return true if 2 groups of clustering centers differ not too much
     * @throws IOException
     */
    public static boolean AreCentersConverged(String oldCenterPath, String newCenterPath) throws IOException {

        List<ArrayList<Double>> oldCenters = Tools.getDataFromHDFS(oldCenterPath, false);
        List<ArrayList<Double>> newCenters = Tools.getDataFromHDFS(newCenterPath, true);

        double allCentersDistance = 0.0;
        for (int i = 0; i < oldCenters.size(); i++) {
            ArrayList<Double> iterOld = oldCenters.get(i);
            ArrayList<Double> iterNew = newCenters.get(i);

            allCentersDistance += calculateDistanceOf2Point(iterOld, iterNew);
        }
        return allCentersDistance <= 0.001;
    }


    /**
     * delete HDFS path if exists
     *
     * @param pathString
     * @throws IOException
     */
    public static void deletePathIfExist(String pathString) throws IOException {
        Path path = new Path(pathString);
        FileSystem hdfs = path.getFileSystem(new Configuration());
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }
}