package pa3;

import scala.Tuple2;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class WikipediaPageRank {

    private static final double BETA = 0.85;
    private static final Pattern SEMICOLON = Pattern.compile(": ");
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final int TOTALITERATIONS = 25;

    boolean useTaxation, wikipediaBomb;
    SparkSession spark;
    String linksFile, titlesFile, outputFile;

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double left, Double right) {
            return left + right;
        }
    }

    public WikipediaPageRank(String profile, String linksFile, String titlesFile, String outputFile) {
        useTaxation = false;
        wikipediaBomb = false;
        /* Profiles
         * A1: No taxation, no Wikipedia bomb
         * A2: No taxation, Wikipedia bomb
         * B1: Taxation, no Wikipedia bomb
         * B2: Taxation, Wikipedia bomb
         */
        switch(profile.toUpperCase()) {
            case "A1":
                break;
            case "A2":
                wikipediaBomb = true;
                break;
            case "B1":
                useTaxation = true;
                break;
            case "B2":
                useTaxation = true;
                wikipediaBomb = true;
                break;
            default:
                System.err.println("Unknown profile provided, using A1 as default");
                break;
        }
        this.linksFile = linksFile;
        this.titlesFile = titlesFile;
        this.outputFile = outputFile + profile;
    }

    public int runSpark() throws Exception {
        spark = SparkSession
                .builder()
                .appName("WikipediaPageRank")
                .getOrCreate();
        pageRankAlgorithm();
        spark.stop();
        return 0;
    }

    public void pageRankAlgorithm() throws Exception  {
        //Step 1: Setup
        JavaRDD<String> titles = spark.read().textFile(titlesFile).javaRDD().cache();
        JavaRDD<String> lines = spark.read().textFile(linksFile).javaRDD().cache();
        JavaPairRDD<String, String> links = lines.mapToPair(x -> {
            String[] parts = SEMICOLON.split(x);
            return new Tuple2<>(parts[0], parts[1]);
        }).cache();
        //Step 1.5: Make "Wikipedia Bomb"
        if (wikipediaBomb) {
            //TODO: Create Wikipedia Bomb for "Rocky Mountain National Park"
            //TODO: Filter for "Rocky Mountain National Park" and articles containing "surfing"?
        }
        double numPages = titles.count();
        JavaPairRDD<String, Double> ranks = links.mapValues(v -> 1.0 / numPages).cache();

        // Step 2: Page Rank Algorithm (Non-Matrix Style)
        int currentIteration = 1;
        while (currentIteration <= TOTALITERATIONS) {
            JavaPairRDD<String, Double> tempRanks = links.join(ranks).values().flatMapToPair(x -> {
                String[] urls = SPACE.split(x._1);
                int numOutLinks = urls.length;
                double rank = x._2;
                List<Tuple2<String, Double>> results = new ArrayList<>();
                for (String n : urls) {
                    results.add(new Tuple2<>(n, rank / numOutLinks));
                }
                return results.iterator();
            });
            ranks = tempRanks.reduceByKey(new Sum());
            if (useTaxation) {
                ranks = ranks.mapValues(v -> (v * BETA) + ((1.0 - BETA) / numPages));
            }
            currentIteration++;
        }

        //Step 3: Map article id to title, sort articles by page rank (descending)
        List<String> titleList = titles.collect();
        JavaPairRDD<String, Double> output = ranks.sortByKey().mapToPair(x -> {
            int id = Integer.parseInt(x._1);
            String title = titleList.get(id - 1);
            return new Tuple2<>(title, x._2);
        });
        output = output.mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap).coalesce(1);
        if (wikipediaBomb) {
            // TODO: Filter by Wikipedia Bomb?
        }
        output.saveAsTextFile(outputFile);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: WikipediaPageRank <profile> <links-file> <titles-file> <output-file>");
            System.exit(1);
        }
        System.out.println("Running Application: WikipediaPageRank");
        WikipediaPageRank sparkProgram = new WikipediaPageRank(args[0], args[1], args[2], args[3]);
        int exitCode = sparkProgram.runSpark();
        System.exit(exitCode);
    }
}
