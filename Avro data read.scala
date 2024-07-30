import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.avro.SchemaConverters

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("KafkaToDataFrame")
  .getOrCreate()

// Schema Registry configurations
val schemaRegistryUrl = "http://localhost:8081"
val topicName = "your_topic_name"
val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(s"$topicName-value")
val avroSchema = schemaMetadata.getSchema

// Convert Avro schema to Spark schema
val parser = new Schema.Parser()
val avroSchemaParsed = parser.parse(avroSchema)
val sparkSchema = SchemaConverters.toSqlType(avroSchemaParsed).dataType.asInstanceOf[StructType]

// Function to convert GenericRecord to Row
def genericRecordToRow(record: GenericRecord, schema: Schema): Row = {
  val values = schema.getFields.toArray.map { field =>
    val fieldName = field.asInstanceOf[Schema.Field].name()
    record.get(fieldName)
  }
  Row(values: _*)
}

// Convert RDD[ConsumerRecord[String, GenericRecord]] to RDD[Row]
val rowRDD = kafkaRDD.map(record => genericRecordToRow(record.value(), avroSchemaParsed))

// Create DataFrame
val df = spark.createDataFrame(rowRDD, sparkSchema)

// Show the DataFrame
df.show()

//------------------


import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("KafkaToDataFrame")
  .getOrCreate()

// Sample schema definition (customize based on your GenericRecord structure)
val schema = StructType(Array(
  StructField("field1", StringType, nullable = true),
  StructField("field2", IntegerType, nullable = true)
  // Add more fields as required
))

// Function to convert GenericRecord to Row
def genericRecordToRow(record: GenericRecord): Row = {
  Row(
    record.get("field1").toString, 
    record.get("field2").asInstanceOf[Int]
    // Add more fields as required
  )
}

// Convert RDD[ConsumerRecord[String, GenericRecord]] to RDD[Row]
val rowRDD = kafkaRDD.map(record => genericRecordToRow(record.value()))

// Create DataFrame
val df = spark.createDataFrame(rowRDD, schema)

// Show the DataFrame
df.show()

