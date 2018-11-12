package info.nourian.Configuration

import java.nio.ByteBuffer

import akka.serialization.{ByteBufferSerializer, SerializerWithStringManifest}

//Still not used in project. It makes serialization/deserialization faster
class MessageSerializer extends SerializerWithStringManifest with ByteBufferSerializer {

  override def identifier: Int = 2011105

  override def manifest(o: AnyRef): String = o.getClass.getName

  override def toBinary(o: AnyRef): Array[Byte] = {
    val buf = ByteBuffer.allocate(256)
    toBinary(o, buf)
    buf.flip()
    val bytes = new Array[Byte](buf.remaining)
    buf.get(bytes)
    bytes
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    fromBinary(ByteBuffer.wrap(bytes), manifest)

  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = ???

  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = ???
}
