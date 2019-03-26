package org.training.spark.streaming.spark_streaming

import java.util.Properties
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by liulebin on 03/10/2019.
  */

case class KafkaProducerProxy(brokerList: String,
                            producerConfig: Properties = new Properties,
                            defaultTopic: Option[String] = None,
                            producer: Option[KafkaProducer[String, String]] = None) {

  type Key = String
  type Val = String

  require(brokerList == null || !brokerList.isEmpty, "Must set broker list")

  private val p = producer getOrElse {

    var props:Properties= new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    new KafkaProducer[String,String](props)
  }


  //把我的消息包装成了ProducerRecord
  private def toMessage(value: Val, key: Option[Key] = None, topic: Option[String] = None): ProducerRecord[Key, Val] = {
    val t = topic.getOrElse(defaultTopic.getOrElse(throw new IllegalArgumentException("Must provide topic or default topic")))
    require(!t.isEmpty, "Topic must not be empty")
    key match {
      case Some(k) => new ProducerRecord(t, k, value)
      case _ => new ProducerRecord(t, value)
    }
  }

  def send(key: Key, value: Val, topic: Option[String] = None) {
    //调用KafkaProducer他的send方法发送消息
    p.send(toMessage(value, Option(key), topic))
  }

  def send(value: Val, topic: Option[String]) {
    send(null, value, topic)
  }

  def send(value: Val, topic: String) {
    send(null, value, Option(topic))
  }

  def send(value: Val) {
    send(null, value, None)
  }

  def shutdown(): Unit = p.close()

}


abstract class KafkaProducerFactory(brokerList: String, config: Properties, topic: Option[String] = None)
    extends Serializable {
  def newInstance(): KafkaProducerProxy
}

class BaseKafkaProducerFactory(brokerList: String,
                                  config: Properties = new Properties,
                                  defaultTopic: Option[String] = None)
  extends KafkaProducerFactory(brokerList, config, defaultTopic) {
  override def newInstance() = new KafkaProducerProxy(brokerList, config, defaultTopic)

}

// 继承一个基础的连接池，需要提供池化的对象类型
class PooledKafkaProducerAppFactory(val factory: KafkaProducerFactory)
  extends BasePooledObjectFactory[KafkaProducerProxy] with Serializable {

  // 用于池来创建对象
  override def create(): KafkaProducerProxy = factory.newInstance()

  // 用于池来包装对象
  override def wrap(obj: KafkaProducerProxy): PooledObject[KafkaProducerProxy] = new DefaultPooledObject(obj)

  // 用于池来销毁对象
  override def destroyObject(p: PooledObject[KafkaProducerProxy]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}
