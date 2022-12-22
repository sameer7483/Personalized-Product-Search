case class Reviews(
  overall: Double,
  vote: String,
  verified: Boolean,
  reviewTime: String,
  reviewerID: String,
  asin: String,
  reviewerName: String,
  reviewText: String,
  summary: String,
  unixReviewTime: Long
)

case class Metadata(
  asin: String,
  title: String,
  description: Array[String],
  price: String,
  brand: String,
  category: Array[String],
  also_buy: Array[String]
)


val reviews = spark.read.json("project/Books.json").as[Reviews]
println("reviews count = " + reviews.count.toString)


val metadata = spark.read.json("project/meta_Books.json").as[Metadata]
println("metadata count = " + metadata.count.toString)

val duplicateAsins = metadata.groupBy($"asin").count().filter($"count" > 1).select("asin").map(_.mkString).collect()
println("duplicate asin count = " + duplicateAsins.size.toString)

val filteredMetadata = metadata.filter(!col("asin").isInCollection(duplicateAsins))
println("filtered metadata count = " + filteredMetadata.count.toString)


val joinedDf = reviews.join(filteredMetadata, "asin")
println("joined count = " + joinedDf.count.toString)

val verifiedDf = joinedDf.filter($"verified" === true)
print("verified count = " + verifiedDf.count.toString)

val priceCleanedDf = verifiedDf.withColumn("price", translate($"price", "$,", "").cast("double")).filter($"price" > 0)
println("price cleaned count = " + priceCleanedDf.count.toString)


val ratingsPriceCleanedDf = priceCleanedDf.filter($"overall" >= 1.0 && $"overall" <= 5.0)
println("ratings and price cleaned count = " + ratingsPriceCleanedDf.count.toString)


ratingsPriceCleanedDf.groupBy("asin").count().select("count").describe().show()


val filteredAsins = ratingsPriceCleanedDf.groupBy("asin").count().filter($"count" > 5).select("asin").map(_.mkString).collect()
val finalDf = ratingsPriceCleanedDf.filter(col("asin").isInCollection(filteredAsins))
println("final count = " + finalDf.count.toString)


val outputPath = "project/books-clean.parquet"
finalDf.write.mode("overwrite").parquet(outputPath)