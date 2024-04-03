package weather.stream;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

class WeatherData {
    private String capital;
    private Double avgTempC;

    // getters and setters
    public String getCapital() {
        return capital;
    }

    public void setCapital(String capital) {
        this.capital = capital;
    }

    public Double getAvgTempC() {
        return avgTempC;
    }

    public void setAvgTempC(Double avgTempC) {
        this.avgTempC = avgTempC;
    }
}


public class Stream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark =
                SparkSession.builder().appName("NetworkWordCount").master("local[*]").getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to
        // localhost:9999
        // Dataset<String> lines = spark.readStream().format("socket").option("host", "localhost")
        //         .option("port", 9999).load().as(Encoders.STRING());

        Dataset<String> lines = spark.readStream().format("socket").option("host",
        "172.28.14.220")
        .option("port", 9999).load().as(Encoders.STRING());

        // Split the lines into words
        Dataset<WeatherData> df = lines.map((MapFunction<String, WeatherData>) x -> {
            String[] parts = x.split(",");
            WeatherData data = new WeatherData();
            data.setCapital(parts[1]);
            data.setAvgTempC(Double.parseDouble(parts[4]));
            return data;
        }, Encoders.bean(WeatherData.class));

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("my_table");

        // Get the max value of avgTempC
        Dataset<Row> maxTemp = spark
                .sql("SELECT capital, MAX(avgTempC) as max_temp FROM my_table GROUP BY capital");

        // Start running the query that prints the running counts to the console
        StreamingQuery query = maxTemp.writeStream().outputMode("complete").format("console")
                .trigger(Trigger.ProcessingTime("1 second")).start();

        query.awaitTermination();
    }
}
