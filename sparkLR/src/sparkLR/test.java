package sparkLR;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

public class test {
	static double []weight = {-100000,264.257288,73.9969150,-35.4461049,83.2698824,1.56642937,-1.92199500,1.26929736};
	public static void main (String[]args) throws StreamingQueryException, InterruptedException {
		SparkSession spark = SparkSession.builder()
				.appName("Spark Linear Regerssion")
				.config("spark.master", "local")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");

		StructType schema_ = new StructType()
				.add("h0", "double")
				.add("h1", "double")
				.add("h2", "double")
				.add("h3", "double")
				.add("h4", "double")
				.add("h5", "double")
				.add("h6", "double")
				.add("h7", "double");

		Dataset<Row> df_csv = spark.readStream()
				//.format("csv")
				.option("sep", " ")
				.schema(schema_)
				.csv("/Users/kimyujin/eclipse-maven-test/sparkLR/src");
				//.load("/Users/kimyujin/eclipse-maven-test/sparkLR/src");
		
		

		Dataset<Row> result = df_csv.withColumn("result", ((df_csv.col("h0").$times(weight[0])).$plus
				(df_csv.col("h1").$times(weight[1])).$plus
				(df_csv.col("h2").$times(weight[2])).$plus
				(df_csv.col("h3").$times(weight[3])).$plus
				(df_csv.col("h4").$times(weight[4])).$plus
				(df_csv.col("h5").$times(weight[5])).$plus
				(df_csv.col("h6").$times(weight[6])).$plus
				(df_csv.col("h7").$times(weight[7]))
				));
		
		
		Dataset<Double> result2 = result.map((MapFunction<Row,Double>) value -> value.getDouble(8),Encoders.DOUBLE());
	
	
	
		result2.writeStream()
		//.format("console")
		.format("csv")        // can be "orc", "json", "csv", "parquat"
		.option("checkpointLocation", "/Users/kimyujin/eclipse-maven-test/sparkLR/src/test/CP")
	    .option("path", "/Users/kimyujin/eclipse-maven-test/sparkLR/src/test")
	    //.trigger(Trigger.ProcessingTime(1))
		.start()
		.awaitTermination();

		
		//위에 주석 처리하고 이것으로 parquet 파일로 저장된 데이터를 볼 수 있다 -> 정상적으로 저장된 것확
		//Dataset<Row> d= spark.read().parquet("/Users/kimyujin/eclipse-maven-test/sparkLR/src/test");
		//d.show();
	}
}
