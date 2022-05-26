import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties


object FlinkConsumer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    // 定义kafka 属性
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop01:9092")
    properties.put("zookeeper.connect", "hadoop01:2181")
    properties.put("group.id", "mytest")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset", "latest")
    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](
      "test", // topic
      new SimpleStringSchema(),
      properties
    )
    // 读取kafka数据
    val kafkaStream: DataStream[String] = env.addSource(flinkKafkaConsumer)
//    val resultValue = kafkaStream
//      .map(data => {
//        val strArr: Array[String] = data.split(",")
//        SensorReading(strArr(0).trim, strArr(1).trim.toLong, strArr(2).trim.toDouble)
//
//      })
    // 写入到 mysql
//    resultValue.addSink(new MysqlJDBCSink)
    kafkaStream.print()
    env.execute("kafka to mysql")
  }

}

/**
 * 创建样例类, 温度传感器 sensor
 */
case class SensorReading(id: String, timestamp: Long, temperature: Double)



/**
 * 自定义 mysql jdbc sink
 */
class MysqlJDBCSink() extends RichSinkFunction[SensorReading] {
  // 自定义连接, 预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // open 上下文管理创建数据库连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sensor_db", "root", "")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // 调用连接，执行 sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.executeBatch()
    }

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}