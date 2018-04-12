package bigdata.project.system

import java.util
import java.util.Objects

/**
  * A simple bean that can be JSON serialized via Jersey. Represents a KafkaStreams instance
  * that has a set of state stores. See {@link RPCService} for how it is used.
  *
  * We use this JavaBean based approach as it fits nicely with JSON serialization provided by
  * jax-rs/jersey
  */
class HostStoreInfo(host: String, port: Int, storeNames: util.Set[String]) {
  def getHost: String = host
  def getPort: Int = port

  def getStoreNames: util.Set[String] = storeNames


  override def toString: String = "{ " + "\"HostStoreInfo\" :" + "{" + "\"host\": \"" + host + "\"," + "\"port\": \"" + port + "\"," + "\"storeNames\": " + storeNames + '}'

  def equals(o: HostStoreInfo): Boolean = {
    if (this eq o) return true
    if (o == null || (getClass ne o.getClass)) return false
    val that = o.asInstanceOf[HostStoreInfo]
    port == that.port && Objects.equals(host, that.host) && Objects.equals(storeNames, that.storeNames)
  }
  override def hashCode: Int = Objects.hash(host, port, storeNames)
}
