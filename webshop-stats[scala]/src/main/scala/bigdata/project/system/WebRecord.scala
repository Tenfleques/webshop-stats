package bigdata.project.system

import org.apache.kafka.streams.KeyValue


//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
class WebRecord (val rec: String) {
  private val record = rec.split(",")
  private val PURCHASES_INDEX = 5
  private val PRICE_INDEX = 6

  def getCountPair(keyIndex: Integer): KeyValue[String, String] = {
    val `val` = 1L
    new KeyValue[String, String](this.record(keyIndex), `val`.toString)
  }

  def getPurchasesCount(keyIndex: Integer) = new KeyValue[String, String](this.record(keyIndex), this.record(PURCHASES_INDEX))

  def getPurchasesValue(keyIndex: Integer): KeyValue[String, String] = {
    val value = this.record(PRICE_INDEX).toLong * this.record(PURCHASES_INDEX).toLong
    new KeyValue[String, String](this.record(keyIndex), value.toString)
  }
}
