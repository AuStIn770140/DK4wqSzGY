// 代码生成时间: 2025-09-22 15:06:33
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class LogParserApp {
    
    public static void main(String[] args) {
        // Initialize Spark Session
        SparkSession spark = SparkSession
            .builder()
            .appName("Log Parser Application")
            .getOrCreate();
        
        try {
            // Check if the input path is provided
            if (args.length < 1) {
                System.err.println("Usage: LogParserApp <log file path>");
                System.exit(1);
            }
            
            String logFilePath = args[0];

            // Read log file as a Dataset and show schema
            Dataset<Row> logData = spark.read.textFile(logFilePath);
            logData.printSchema();
            
            // Parse log data into a structured format
            Dataset<Row> parsedLogs = parseLogs(logData);

            // Show the parsed log data
            parsedLogs.show();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Stop the Spark session
            spark.stop();
        }
    }
    
    /**
     * Parse the log data into a structured format.
     * 
     * @param logs The Dataset of log lines.
     * @return The Dataset of parsed log data.
     */
    private static Dataset<Row> parseLogs(Dataset<Row> logs) {
        // Define a User Defined Function (UDF) to parse a log line into a structured format
        functions.Udf2<String, Row, Row> parseLogUDF = (String logLine, Row row) -> {
            String[] parts = logLine.split("\s+");
            if (parts.length < 4) {
                return null; // Skip rows that do not conform to expected log format
            }
            
            // Assuming the log format is: IP timestamp request-status bytes
            String ip = parts[0];
            String timestamp = parts[1];
            String requestStatus = parts[2];
            int bytes = Integer.parseInt(parts[3]);
            
            return RowFactory.create(ip, timestamp, requestStatus, bytes);
        };
        
        // Apply the UDF to each log line
        Dataset<Row> parsedLogs = logs.withColumn("parsedLog", functions.udf(parseLogUDF, RowEncoder.apply(new StructType()
            .add("ip", "string")
            .add("timestamp", "string")
            .add("requestStatus", "string")
            .add("bytes", "integer"))).apply(logs.col("value"), logs.row());
        
        return parsedLogs.select("parsedLog.*");
    }
}