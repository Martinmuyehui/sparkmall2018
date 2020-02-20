import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.HashMap

object MapTest {
  def main(args: Array[String]): Unit = {

//    val t1: (String, Int, String, Boolean) = ("a",1,"2",true)
//    println(t1.hashCode())

//    var stringToLong = new HashMap[String,Long]()
//    map1 = HashMap("city1"->13L,"city2" ->4L)

    var maptest1 = Map("city1"->13L,"city2" ->4L,"city3" ->5L)
//    println(stringToLong("city1"))
    println(maptest1.get("city1").get)
//    println(map1.get("city1"))
//    println(map1.getOrElse("city1", 0L))
    maptest1 += ("c1" -> 9L,"c2" -> 90L)
    val stringToLong: Map[String, Long] = maptest1 + ("c"->0L)

    val arr: mutable.Buffer[Int] = Array(1,2,3,4,5).toBuffer
    println(arr.reduce(_ + _))
//    for (i <- 0 until arr.length) {
//      println(i + ":" + arr(i))
//    }
//    println(arr)

    val l: Long = System.currentTimeMillis()
    val date: Date = new Date(l)
    val str2: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    val str1: String = new SimpleDateFormat("yyyy-MM-dd").format(l)
    println(str1)
    println(str2)
    println( date)
  }

}
