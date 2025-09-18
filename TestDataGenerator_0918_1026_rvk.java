// 代码生成时间: 2025-09-18 10:26:22
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.util.Random;

public class TestDataGenerator {

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession
            .builder()
            .appName("Test Data Generator")
            .master("local[*]")
            .getOrCreate();

        try {
            // Define the number of records to generate
            int numberOfRecords = 100; // This can be parameterized

            // Generate test data
            Dataset<Row> testData = generateTestData(spark, numberOfRecords);

            // Show the generated test data
            testData.show();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Stop the Spark session
            spark.stop();
        }
    }

    /**
     * Generates a dataset of random test data.
     * 
     * @param spark The Spark session
     * @param numberOfRecords The number of records to generate
     * @return A dataset containing the generated test data
     */
    private static Dataset<Row> generateTestData(SparkSession spark, int numberOfRecords) {
        // Create a random number generator
        Random random = new Random();

        // Initialize an empty list to store test data
        java.util.List<Tuple2<Integer, String>> data = new java.util.ArrayList<>();

        // Generate the specified number of records
        for (int i = 0; i < numberOfRecords; i++) {
            // Generate random data for each record
            int id = random.nextInt(1000);
            String name = "Name" + id;
            data.add(new Tuple2<>(id, name));
        }

        // Create a Spark DataFrame from the generated data
        return spark.createDataset(data, Encoders.tuple(Encoders.INT(), Encoders.STRING()))
            .toDF("id", "name");
    }
}
