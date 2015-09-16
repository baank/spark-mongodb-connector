package nsmc.rdd.partitioner

import nsmc.mongo.{MongoInterval, IntervalGenerator, CollectionConfig}
import org.apache.spark.Partition

private[nsmc]
class MongoRDDPartitioner(val collectionConfig: CollectionConfig) extends nsmc.Logging {
  val ig = new IntervalGenerator(collectionConfig.connectorConf.getDestination(),
    collectionConfig.databaseName, collectionConfig.collectionName)

  def makePartitions(): Array[Partition] = {
    val intervals = ig.generateSyntheticIntervals(collectionConfig.connectorConf.splitSize, Seq(("_id", 1)))
    val partitions = intervals.zipWithIndex map {
      case (interval, index) => {
        val p: Partition = new MongoRDDPartition(index, 0, interval)
        p
      }
    }
    partitions.to[Array]
  }

  def close() : Unit = {
    ig.close();
  }
}
