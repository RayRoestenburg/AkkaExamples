package integration

import org.scalatest.Spec
import se.scalablesolutions.akka.persistence.redis.RedisStorage
import se.scalablesolutions.akka.stm.local._
import org.scalatest.matchers.{ShouldMatchers, MustMatchers}

/**
 * Test for storing bytes in redis, that used to fail in a previous version.
 * Redis needs to run for this test.
 */
class RedisByteSpecs extends Spec with ShouldMatchers {
  private lazy val storage = atomic {RedisStorage.getMap("redis-bytes-test")}

  describe("Redis backed persistent map") {

    describe("(when it receives data with bytes 0x10 and 0x13)") {
      it("should store the bytes correctly") {

        val byteKey: Array[Byte] = Array(0x10, 0x13, 55, 56, 57, 0x10, 0x13)
        val data: Array[Byte] = Array(55, 56, 57, 0x10, 58, 0x13, 59, 0x10, 0x13)
        atomic {
          storage.put(byteKey, data)
          storage.commit
        }
        val result = storage.get(byteKey)
        val predicate =  (x:Byte, y:Byte) => true
        result match {
          case Some(bytes:Array[Byte]) =>  {bytes.corresponds(data)(predicate)  should equal (true)}
          case _ => fail("not found")
        }
      }
    }
  }
}