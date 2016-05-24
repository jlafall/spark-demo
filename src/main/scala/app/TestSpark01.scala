package spark.test.app
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time._
import spark.test.models.StockData

object TestSpark01 {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Spark Test #01")
		val context = new SparkContext(conf)

		// get lines from stock data CSV file
		val lines = context.textFile("test-data.csv")

		// get cached stock data from CSV formatted columns
		val stocks = lines.map(line => line.split(","))
			.map(columns => columns.map(_.trim))
			.map {
				columns => StockData (
					new DateTime(columns(0)),
					BigDecimal(columns(1)),
					BigDecimal(columns(4)),
					BigDecimal(columns(6))
				)
			}
			.cache()

		// write another CSV with the dates grouped by yyyy-MM with 
		// fields summed together
		stocks.map(stock => (stock.date.toString("yyyy-MM"), stock))
			.reduceByKey {
				(s1, s2) => StockData (
					s1.date, 
					s1.open + s2.open, 
					s1.close + s2.close, 
					s1.adjClose + s2.adjClose,
					s1.totalCount + s2.totalCount
				)
			}
			.sortByKey(ascending = false)
			.map { 
				case (k, s) => s"""${k}, ${s.open}, ${s.close}, ${s.adjClose}, ${s.totalCount}"""
			}
			.saveAsTextFile("test-result")
	}
}