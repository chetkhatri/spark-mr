// Assignment: Apache Spark runs on top of HDFS files

// Reading file from HDFS
val source = sqlContext.read.json("file:///home/chetan306/Documents/spark-mr/source.json")
// Saving in temp table
source.registerTempTable("source")
// Dataframe transformations
var sqldf = sqlContext.sql("select distinct a.* from source a inner join source b where a.PRODUCT_ID = b.PRODUCT_ID and b.TRANSACTION_TYPE='Sell' order by a.DATE")

var dfBuy = sqldf.filter($"TRANSACTION_TYPE" === "Buy")
var dfSell = sqldf.filter($"TRANSACTION_TYPE" === "Sell")

dfBuy.registerTempTable("dfBuy")
dfSell.registerTempTable("dfSell")
// Spark Transformations and action for Map Reduce 
var price = sqlContext.sql("SELECT ((b.QUANTITY - s.QUANTITY) * (s.PRICE - b.PRICE)) as PROFIT from dfBuy b, dfSell s where b.PRODUCT_ID = s.PRODUCT_ID order by 1").take(dfSell.count.toInt)
var totalProfit = price.map(row => row.getLong(0)).reduce((a,b) => a+b)
// Printing total Profit
print(totalProfit)