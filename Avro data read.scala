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


import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.SchemaConverters

import java.util.Properties
import scala.collection.JavaConverters._

object KafkaAvroToDataFrame {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaToDataFrame")
      .getOrCreate()

    // Kafka consumer configuration
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "your_group_id")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    props.put("schema.registry.url", "http://localhost:8081")
    props.put("specific.avro.reader", "false")  // Use GenericRecord

    val consumer = new KafkaConsumer[String, GenericRecord](props)
    consumer.subscribe(java.util.Arrays.asList("your_topic_name"))

    // Fetch schema from Schema Registry
    val schemaRegistryUrl = "http://localhost:8081"
    val topicName = "your_topic_name"
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(s"$topicName-value")
    val avroSchema = new Schema.Parser().parse(schemaMetadata.getSchema)
    val sparkSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    // Function to convert GenericRecord to Row
    def genericRecordToRow(record: GenericRecord, schema: Schema): Row = {
      val values = schema.getFields.toArray.map { field =>
        val fieldName = field.asInstanceOf[Schema.Field].name()
        record.get(fieldName) match {
          case utf8: org.apache.avro.util.Utf8 => utf8.toString
          case other => other
        }
      }
      Row(values: _*)
    }

    // Consume messages from Kafka
    val records = consumer.poll(1000).asScala

    // Convert RDD[ConsumerRecord[String, GenericRecord]] to RDD[Row]
    val rowRDD = spark.sparkContext.parallelize(records.map(record => genericRecordToRow(record.value(), avroSchema)).toSeq)

    // Create DataFrame
    val df = spark.createDataFrame(rowRDD, sparkSchema)

    // Show the DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

object KafkaAvroToDataFrame {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaToDataFrame")
      .getOrCreate()

    // Assuming you have the messageRdd already created
    val messageRdd: org.apache.spark.rdd.RDD[ConsumerRecord[String, GenericRecord]] = ??? // Replace with your RDD initialization

    // Schema Registry configurations
    val schemaRegistryUrl = "http://localhost:8081"
    val topicName = "your_topic_name"
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(s"$topicName-value")
    val avroSchema = new Schema.Parser().parse(schemaMetadata.getSchema)
    val sparkSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    // Convert GenericRecord to Row within a serializable object
    object GenericRecordConverter extends Serializable {
      def genericRecordToRow(record: GenericRecord, schema: Schema): Row = {
        val values = schema.getFields.toArray.map { field =>
          val fieldName = field.asInstanceOf[Schema.Field].name()
          record.get(fieldName) match {
            case utf8: org.apache.avro.util.Utf8 => utf8.toString
            case other => other
          }
        }
        Row(values: _*)
      }
    }

    // Convert RDD[ConsumerRecord[String, GenericRecord]] to RDD[Row]
    val rowRDD = messageRdd.map(record => GenericRecordConverter.genericRecordToRow(record.value(), avroSchema))

    // Create DataFrame
    val df = spark.createDataFrame(rowRDD, sparkSchema)

    // Show the DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}



import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

object KafkaAvroToDataFrame {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaToDataFrame")
      .getOrCreate()

    // Assuming you have the messageRdd already created
    val messageRdd: org.apache.spark.rdd.RDD[ConsumerRecord[String, GenericRecord]] = ??? // Replace with your RDD initialization

    // Schema Registry configurations
    val schemaRegistryUrl = "http://localhost:8081"
    val topicName = "your_topic_name"
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(s"$topicName-value")
    val avroSchema = new Schema.Parser().parse(schemaMetadata.getSchema)
    val sparkSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    // Convert GenericRecord to Row within a serializable object
    object GenericRecordConverter extends Serializable {
      def genericRecordToRow(record: GenericRecord, schema: Schema): Row = {
        val values = schema.getFields.toArray.map { field =>
          val fieldName = field.asInstanceOf[Schema.Field].name()
          record.get(fieldName) match {
            case utf8: org.apache.avro.util.Utf8 => utf8.toString
            case other => other
          }
        }
        Row(values: _*)
      }
    }

    // Convert RDD[ConsumerRecord[String, GenericRecord]] to RDD[Row]
    val rowRDD = messageRdd.map(record => GenericRecordConverter.genericRecordToRow(record.value(), avroSchema))

    // Create DataFrame
    val df = spark.createDataFrame(rowRDD, sparkSchema)

    // Show the DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}


//-------


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.common.serialization.StringDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.spark.SparkContext

object KafkaAvroToDataFrame {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaToDataFrame")
      .getOrCreate()

    val sc = spark.sparkContext

    // Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[KafkaAvroDeserializer],
      "schema.registry.url" -> "http://localhost:8081",
      "group.id" -> "your_group_id",
      "specific.avro.reader" -> "false",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the topic and offset ranges
    val topics = Array("your_topic_name")
    val offsetRanges = Array(
      OffsetRange("your_topic_name", 0, 0, 100) // Replace with actual offset ranges
    )

    // Create RDD from Kafka
    val kafkaRDD = KafkaUtils.createRDD[String, GenericRecord](
      sc,
      kafkaParams,
      offsetRanges,
      PreferConsistent
    )

    // Schema Registry configurations
    val schemaRegistryUrl = "http://localhost:8081"
    val topicName = "your_topic_name"
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(s"$topicName-value")
    val avroSchema = new Schema.Parser().parse(schemaMetadata.getSchema)
    val sparkSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    // Convert GenericRecord to Row within a serializable object
    object GenericRecordConverter extends Serializable {
      def genericRecordToRow(record: GenericRecord, schema: Schema): Row = {
        val values = schema.getFields.toArray.map { field =>
          val fieldName = field.asInstanceOf[Schema.Field].name()
          record.get(fieldName) match {
            case utf8: org.apache.avro.util.Utf8 => utf8.toString
            case other => other
          }
        }
        Row(values: _*)
      }
    }

    // Debug: Print messages to verify deserialization
    kafkaRDD.foreach { message =>
      println(s"Key: ${message.key()}, Value: ${message.value()}")
    }

    // Convert RDD[ConsumerRecord[String, GenericRecord]] to RDD[Row]
    val rowRDD = kafkaRDD.map(record => GenericRecordConverter.genericRecordToRow(record.value(), avroSchema))

    // Create DataFrame
    val df = spark.createDataFrame(rowRDD, sparkSchema)

    // Show the DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}






// Define the string containing special characters separated by '|'
val specialCharsString = ",|#|#|*|\\|$"

// Convert the string to a list of characters
val specialCharsList = specialCharsString.split("\\|").toList

// Create a regex pattern from the special characters
val pattern = specialCharsList.map("\\Q" + _ + "\\E").mkString("|").r

// Define the input string where you want to replace these characters
val inputString = "Hello, World! Welcome to Scala #1 with *special* characters\\$"

// Replace each occurrence of the special characters with '@'
val resultString = pattern.replaceAllIn(inputString, "@")

// Print the result
println(resultString)



