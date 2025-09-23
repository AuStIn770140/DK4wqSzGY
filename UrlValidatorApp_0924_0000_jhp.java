// 代码生成时间: 2025-09-24 00:00:35
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.net.URL;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UrlValidatorApp {

    // Main method to start the Spark application
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession
            .builder()
            .appName("UrlValidatorApp")
            .getOrCreate();

        // Get the Spark context from the Spark session
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Check if the arguments are provided
        if (args.length == 0) {
            System.err.println("Please provide the path to the URL file");
            System.exit(1);
        }

        // Load the URLs from the provided file path
        String urlFilePath = args[0];
        JavaRDD<String> urls = sc.textFile(urlFilePath);

        // Validate the URLs and collect the results
        List<ValidationResult> results = urls.map(url -> validateUrl(url)).collect();

        // Stop the Spark context
        sc.close();

        // Print the results
        results.forEach(System.out::println);
    }

    // Method to validate a single URL
    private static ValidationResult validateUrl(String url) {
        try {
            URL urlObj = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();

            // Set the request method to HEAD to avoid downloading the content
            connection.setRequestMethod("HEAD");
            connection.connect();

            // Check the HTTP response code to determine if the URL is valid
            int responseCode = connection.getResponseCode();
            if (responseCode >= 200 && responseCode < 300) {
                return new ValidationResult(url, true, "URL is valid");
            } else {
                return new ValidationResult(url, false, "URL is invalid: HTTP response code " + responseCode);
            }
        } catch (InterruptedIOException e) {
            return new ValidationResult(url, false, "URL validation timed out");
        } catch (IOException e) {
            return new ValidationResult(url, false, "URL is invalid: IOException occurred");
        }
    }

    // Helper class to store the validation result
    public static class ValidationResult {
        private String url;
        private boolean isValid;
        private String message;

        public ValidationResult(String url, boolean isValid, String message) {
            this.url = url;
            this.isValid = isValid;
            this.message = message;
        }

        @Override
        public String toString() {
            return "URL: " + url + ", Valid: " + isValid + ", Message: " + message;
        }
    }
}
