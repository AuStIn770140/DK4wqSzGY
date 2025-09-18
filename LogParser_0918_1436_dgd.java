// 代码生成时间: 2025-09-18 14:36:53
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class LogParser {

    // Main method to run the log parser
    public static void main(String[] args) {
        // Check for valid command line arguments
        if (args.length < 1) {
            System.err.println("Usage: LogParser <log file path> <output path>");
            System.exit(1);
        }

        // Create a Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("LogParser")
                .getOrCreate();

        // Set the log file path and output path from command line arguments
        String logFilePath = args[0];
        String outputPath = args.length > 1 ? args[1] : "logs_parsed";

        try {
            // Read the log file into a DataFrame
            Dataset<Row> logData = spark.read().textFile(logFilePath)
                // Split the log entry into columns using a regular expression
                .selectExpr("split(value, ' ') as tokens")
                .selectExpr("tokens[0] as ip",
                         