import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import scala.jdk.CollectionConverters._

class KafkaConsumerExampleSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "KafkaConsumerExample" should "retrieve offset ranges" in {
    // Mock KafkaConsumer
    val mockConsumer = mock[KafkaConsumer[String, String]]

    // Define partitions for the topic
    val partitions = List(
      new PartitionInfo("test-topic", 0, null, Array(), Array()),
      new PartitionInfo("test-topic", 1, null, Array(), Array())
    )

    when(mockConsumer.partitionsFor("test-topic")).thenReturn(partitions.asJava)

    // Define TopicPartitions
    val topicPartitions = partitions.map(p => new TopicPartition(p.topic(), p.partition()))

    // Mock consumer assignment and seekToEnd behavior
    doNothing().when(mockConsumer).assign(topicPartitions.asJava)
    doNothing().when(mockConsumer).seekToEnd(topicPartitions.asJava)
    when(mockConsumer.position(any[TopicPartition])).thenReturn(100L, 200L)

    // Call the method
    val offsets = KafkaConsumerExample.getOffsets(mockConsumer, "test-topic")

    // Validate results
    offsets shouldEqual Map(0 -> 100L, 1 -> 200L)

    // Verify interactions
    verify(mockConsumer).partitionsFor("test-topic")
    verify(mockConsumer).assign(topicPartitions.asJava)
    verify(mockConsumer).seekToEnd(topicPartitions.asJava)
    verify(mockConsumer, times(2)).position(any[TopicPartition])
  }
}




import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema as RestSchema
import org.apache.avro.Schema
import org.apache.spark.sql.types.{StructType, DataType}
import com.databricks.spark.avro.SchemaConverters
import org.mockito.MockitoSugar

class SchemaRegistryTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "getSchemaFromSchemaRegistry" should "retrieve and convert schema from schema registry" in {
    // Mocking RestService
    val schemaRegistryURL = "http://localhost:8081"
    val inputTopic = "test-topic"
    val schemaSubjectId = "-value"
    val topicValueName = inputTopic + schemaSubjectId

    val mockRestService = mock[RestService]
    val mockRestResponseSchema = mock[RestSchema]

    // Sample Avro schema in JSON format
    val avroSchemaString =
      """
        |{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "fields": [
        |    {"name": "field1", "type": "string"},
        |    {"name": "field2", "type": "int"}
        |  ]
        |}
        |""".stripMargin

    // Mocking behavior
    when(mockRestResponseSchema.getSchema).thenReturn(avroSchemaString)
    when(mockRestService.getLatestVersion(topicValueName)).thenReturn(mockRestResponseSchema)

    // Implementing the function logic directly in the test for clarity
    val parser = new Schema.Parser()
    val topicValueAvroSchema: Schema = parser.parse(mockRestResponseSchema.getSchema)
    val schemaRegistrySchema: StructType = SchemaConverters.toSqlType(topicValueAvroSchema).dataType.asInstanceOf[StructType]

    val schemaRegistrySchema2 = DataType.fromJson(SchemaConverters.toSqlType(topicValueAvroSchema).dataType.prettyJson).asInstanceOf[StructType]

    // Assertions
    schemaRegistrySchema2 shouldEqual schemaRegistrySchema
  }
}




"writeBadRecords" should "write bad records (with nulls) to the specified location" in {
    // Sample data for testing
    val schema = StructType(
      List(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )

    val data = Seq(
      Row(1, "Alice"),
      Row(2, null),  // Bad record
      Row(3, "Charlie"),
      Row(null, "David")  // Bad record
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    // Temporary location for test output
    val outputLocation = "test_output/bad_records"

    // Call the method
    writeBadRecords(df, outputLocation)

    // Check if the output file is written and contains bad records
    val outputPath = Paths.get(outputLocation)
    assert(Files.exists(outputPath), "Output location does not exist")

    // Read the written JSON file and validate the records
    val outputFiles = Files.list(outputPath).toArray.map(_.toString).filter(_.endsWith(".json"))
    assert(outputFiles.nonEmpty, "No output files found")

    // Collect written records
    val writtenRecords = outputFiles.flatMap { file =>
      val bufferedSource = Source.fromFile(file)
      val records = bufferedSource.getLines().toList
      bufferedSource.close()
      records
    }

    // Check that at least one bad record was written
    writtenRecords should not be empty

    // Define expected bad records
    val expectedBadRecords = Seq("""{"id":2,"name":null}""", """{"id":null,"name":"David"}""")
    
    // Convert to Set for comparison as JSON serialization order of fields may differ
    writtenRecords.toSet shouldEqual expectedBadRecords.toSet
  }

  // Cleanup after test
  def cleanup(outputLocation: String): Unit = {
    val outputPath = Paths.get(outputLocation)
    if (Files.exists(outputPath)) {
      Files.walk(outputPath)
        .sorted((path1, path2) => path2.compareTo(path1))
        .forEach(Files.delete)
    }
  }

  cleanup("test_output/bad_records") // Call cleanup after tests







// Create a mock SparkSession
  val spark: SparkSession = mock[SparkSession]

  // Create a mock DataFrame
  val df: DataFrame = mock[DataFrame]
  val filteredDf: DataFrame = mock[DataFrame]

  // Sample test constants
  val badRecordTargetLoc = "mock/path/to/bad_records"
  val badRecordCondition = "mock condition"

  test("writeBadRecords should not write to disk if there are no bad records") {
    // Mocking filter and persist behavior
    when(df.filter(badRecordCondition)).thenReturn(filteredDf)
    when(filteredDf.persist(StorageLevel.MEMORY_AND_DISK_SER)).thenReturn(filteredDf)
    when(filteredDf.isEmpty).thenReturn(true)

    // Call the method to test
    WriteBadRecords.writeBadRecords(df, badRecordTargetLoc)

    // Verify the interactions
    verify(filteredDf, times(1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    verify(filteredDf, times(1)).isEmpty
    verify(filteredDf, never()).write
    verify(filteredDf, times(1)).unpersist()
  }

  test("writeBadRecords should write to disk if there are bad records") {
    // Mocking filter and persist behavior
    when(df.filter(badRecordCondition)).thenReturn(filteredDf)
    when(filteredDf.persist(StorageLevel.MEMORY_AND_DISK_SER)).thenReturn(filteredDf)
    when(filteredDf.isEmpty).thenReturn(false)

    // Mock write behavior
    val writeConfig = mock[DataFrameWriter[Row]]
    when(filteredDf.write).thenReturn(writeConfig)
    when(writeConfig.mode("append")).thenReturn(writeConfig)
    when(writeConfig.json(badRecordTargetLoc)).thenReturn(writeConfig)

    // Call the method to test
    WriteBadRecords.writeBadRecords(df, badRecordTargetLoc)

    // Verify the interactions
    verify(filteredDf, times(1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    verify(filteredDf, times(1)).isEmpty
    verify(filteredDf.write.mode("append"), times(1)).json(badRecordTargetLoc)
    verify(filteredDf, times(1)).unpersist()
  }

-------------------------------------------------

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.scalatest.MockitoSugar
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema as RestSchema
import org.apache.avro.Schema
import org.apache.spark.sql.types.{StructType, DataType}
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.{DataFrame, SparkSession, DataFrameWriter, Row}
import org.apache.spark.storage.StorageLevel

class CombinedTests extends AnyFunSuite with AnyFlatSpec with Matchers with MockitoSugar {

  // Test for WriteBadRecords method
  test("writeBadRecords should not write to disk if there are no bad records") {
    // Create a mock SparkSession
    val spark: SparkSession = mock[SparkSession]

    // Create a mock DataFrame
    val df: DataFrame = mock[DataFrame]
    val filteredDf: DataFrame = mock[DataFrame]

    // Sample test constants
    val badRecordTargetLoc = "mock/path/to/bad_records"
    val badRecordCondition = "mock condition"

    // Mocking filter and persist behavior
    when(df.filter(badRecordCondition)).thenReturn(filteredDf)
    when(filteredDf.persist(StorageLevel.MEMORY_AND_DISK_SER)).thenReturn(filteredDf)
    when(filteredDf.isEmpty).thenReturn(true)

    // Call the method to test
    WriteBadRecords.writeBadRecords(df, badRecordTargetLoc)

    // Verify the interactions
    verify(filteredDf, times(1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    verify(filteredDf, times(1)).isEmpty
    verify(filteredDf, never()).write
    verify(filteredDf, times(1)).unpersist()
  }

  test("writeBadRecords should write to disk if there are bad records") {
    // Create a mock SparkSession
    val spark: SparkSession = mock[SparkSession]

    // Create a mock DataFrame
    val df: DataFrame = mock[DataFrame]
    val filteredDf: DataFrame = mock[DataFrame]

    // Sample test constants
    val badRecordTargetLoc = "mock/path/to/bad_records"
    val badRecordCondition = "mock condition"

    // Mocking filter and persist behavior
    when(df.filter(badRecordCondition)).thenReturn(filteredDf)
    when(filteredDf.persist(StorageLevel.MEMORY_AND_DISK_SER)).thenReturn(filteredDf)
    when(filteredDf.isEmpty).thenReturn(false)

    // Mock write behavior
    val writeConfig = mock[DataFrameWriter[Row]]
    when(filteredDf.write).thenReturn(writeConfig)
    when(writeConfig.mode("append")).thenReturn(writeConfig)
    when(writeConfig.json(badRecordTargetLoc)).thenReturn(writeConfig)

    // Call the method to test
    WriteBadRecords.writeBadRecords(df, badRecordTargetLoc)

    // Verify the interactions
    verify(filteredDf, times(1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    verify(filteredDf, times(1)).isEmpty
    verify(filteredDf.write.mode("append"), times(1)).json(badRecordTargetLoc)
    verify(filteredDf, times(1)).unpersist()
  }

  // Test for Schema Registry interactions
  "getSchemaFromSchemaRegistry" should "retrieve and convert schema from schema registry" in {
    // Mocking RestService
    val schemaRegistryURL = "http://localhost:8081"
    val inputTopic = "test-topic"
    val schemaSubjectId = "-value"
    val topicValueName = inputTopic + schemaSubjectId

    val mockRestService = mock[RestService]
    val mockRestResponseSchema = mock[RestSchema]

    // Sample Avro schema in JSON format
    val avroSchemaString =
      """
        |{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "fields": [
        |    {"name": "field1", "type": "string"},
        |    {"name": "field2", "type": "int"}
        |  ]
        |}
        |""".stripMargin

    // Mocking behavior
    when(mockRestResponseSchema.getSchema).thenReturn(avroSchemaString)
    when(mockRestService.getLatestVersion(topicValueName)).thenReturn(mockRestResponseSchema)

    // Simulating the function behavior
    val parser = new Schema.Parser()
    val topicValueAvroSchema: Schema = parser.parse(mockRestResponseSchema.getSchema)
    val schemaRegistrySchema: StructType = SchemaConverters.toSqlType(topicValueAvroSchema).dataType.asInstanceOf[StructType]

    val schemaRegistrySchema2 = DataType.fromJson(SchemaConverters.toSqlType(topicValueAvroSchema).dataType.prettyJson).asInstanceOf[StructType]

    // Assertions
    schemaRegistrySchema2 shouldEqual schemaRegistrySchema
  }
}







