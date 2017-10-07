/*
 * Copyright 2017 Pronghorn Technology LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.pronghorn.mongodb.bytesbufs

import org.bson.ByteBuf
import tech.pronghorn.server.bufferpools.ManagedByteBuffer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicInteger

class PronghornByteBuf(private val managed: ManagedByteBuffer) : ByteBuf {
    private val buffer = managed.buffer
    private val referenceCount = AtomicInteger(1)
    var alreadyRead = 0

    override fun asNIO(): ByteBuffer = buffer

    override fun clear(): ByteBuf {
        buffer.clear()
        return this
    }

    override fun flip(): ByteBuf {
        buffer.flip()
        return this
    }

    override fun limit(): Int = buffer.limit()

    override fun limit(newLimit: Int): ByteBuf {
        buffer.limit(newLimit)
        return this
    }

    override fun put(index: Int, b: Byte): ByteBuf {
        buffer.put(index, b)
        return this
    }

    override fun put(src: ByteArray, offset: Int, length: Int): ByteBuf {
        buffer.put(src, offset, length)
        return this
    }

    override fun put(b: Byte): ByteBuf {
        buffer.put(b)
        return this
    }

    override fun array(): ByteArray = buffer.array()

    override fun getInt(): Int = buffer.getInt()

    override fun getInt(index: Int): Int = buffer.getInt(index)

    override fun getReferenceCount(): Int = referenceCount.get()

    override fun capacity(): Int = buffer.capacity()

    override fun retain(): ByteBuf {
        referenceCount.incrementAndGet()
        return this
    }

    override fun order(byteOrder: ByteOrder?): ByteBuf {
        buffer.order(byteOrder)
        return this
    }

    override fun getLong(): Long = buffer.getLong()

    override fun getLong(index: Int) = buffer.getLong(index)

    override fun hasRemaining(): Boolean = buffer.hasRemaining()

    override fun remaining(): Int = buffer.remaining()

    override fun position(newPosition: Int): ByteBuf {
        buffer.position(newPosition)
        return this
    }

    override fun position(): Int = buffer.position()

    override fun asReadOnly(): ByteBuf {
        return ChildByteBuf(this, buffer.asReadOnlyBuffer())
    }

    override fun get(): Byte = buffer.get()

    override fun get(index: Int): Byte = buffer.get(index)

    override fun get(bytes: ByteArray?): ByteBuf {
        buffer.get(bytes)
        return this
    }

    override fun get(index: Int, bytes: ByteArray): ByteBuf {
        buffer.get(bytes, index, bytes.size)
        return this
    }

    override fun get(bytes: ByteArray, offset: Int, length: Int): ByteBuf {
        buffer.get(bytes, offset, length)
        return this
    }

    override fun get(index: Int, bytes: ByteArray, offset: Int, length: Int): ByteBuf {
        throw UnsupportedOperationException()
    }

    override fun release() {
        val newCount = referenceCount.decrementAndGet()
        if(newCount == 0){
            managed.release()
        }
    }

    override fun getDouble(): Double = buffer.getDouble()

    override fun getDouble(index: Int): Double = buffer.getDouble(index)

    override fun duplicate(): ByteBuf = ChildByteBuf(this, buffer.duplicate())

    fun slice(): ByteBuf = ChildByteBuf(this, buffer.slice())
}
