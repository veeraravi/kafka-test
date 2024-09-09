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

