package nsmc.mongo


case class CollectionConfig(connectorConf: MongoConnectorConf,
                            databaseName: String,
                            collectionName: String,
                            indexedKeys: Seq[String]) {

}
