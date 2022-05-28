package com.dcits.toDatabase.toMysql

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties


object Main {
  //  val TopicName = "HX_DJ.DZDZ_FPXX_PTFP_FLINK_TEST"
  val TopicName = "test"
  val TablePK = ""
  //  val GroupID = "DZDZ_FPXX_PTFP_FLINK_TEST_1"
  val GroupID = "test-consumer-group"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)  // todo 并行度设置问题
    //    env.enableCheckpointing(5000)
    // 定义kafka 属性
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop01:9092")
    properties.put("zookeeper.connect", "hadoop01:2181")
    properties.put("group.id", GroupID)
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset", "latest")
    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](
      TopicName, // topic
      new SimpleStringSchema(),
      properties
    )
    //设置从最新的offset开始消费
    // flinkKafkaConsumer.setStartFromLatest()
    flinkKafkaConsumer.setStartFromGroupOffsets()
//    flinkKafkaConsumer.
    //自动提交offset
    //    flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    // 读取kafka数据
    val kafkaStream: DataStream[String] = env.addSource(flinkKafkaConsumer)
    val resultValue = kafkaStream
      .map(resData => {
        val dataJsonObj = JSON.parseObject(resData)
        val message = dataJsonObj.getJSONObject("message")
        val operation = message.getJSONObject("headers").getString("operation")
        val SqlBuilder = new BuildSql
        val sqlStr = SqlBuilder.build(tableName = TopicName, pk = TablePK, message = message, operation = operation)
        MsgInfo(sqlStr = sqlStr, operation = operation, message = message)
      })
    // 写入到 mysql
    resultValue.print()
//    resultValue.addSink(new MysqlJDBCSink())
    //    kafkaStream.print()
    env.execute("kafka to mysql")
  }

}


/**
 * 自定义 mysql jdbc sink
 */
class MysqlJDBCSink() extends RichSinkFunction[MsgInfo] {
  // 自定义连接, 预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // open 上下文管理创建数据库连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://10.0.23.109:3306/sensor_db", "root", "123456")
  }

  // 调用连接，执行 sql
  override def invoke(value: MsgInfo, context: SinkFunction.Context[_]): Unit = {
    println(value.sqlStr)
    insertStmt = conn.prepareStatement(value.sqlStr)
    // todo 异常捕获
    val res = insertStmt.execute()
    println(res)
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}