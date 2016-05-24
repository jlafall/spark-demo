package spark.test.models
import org.joda.time._

case class StockData (
	date: DateTime,
	open: BigDecimal,
	close: BigDecimal,
	adjClose: BigDecimal,
	totalCount: Int = 1
)

