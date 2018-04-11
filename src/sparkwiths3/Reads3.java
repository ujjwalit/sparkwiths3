/*package sparkwiths3;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.*;

public class Reads3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder() 
				.master("local") 
				.appName("Spark Session Example")
				.getOrCreate();
		
		 spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId","AKIAJ7P4BLMMFLRMKRRA");
	     spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey","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");
				
			
		
		//AWSAccessKeyId=AKIAJ7P4BLMMFLRMKRRA
        //AWSSecretKey=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

	     Dataset<Row> csv = spark.read().csv("s3n://s3redujjwal/data.csv");
         csv.show();
	}

}*/
