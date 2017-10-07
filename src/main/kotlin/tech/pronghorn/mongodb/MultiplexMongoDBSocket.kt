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

package tech.pronghorn.mongodb

import com.mongodb.ServerAddress
import com.mongodb.connection.*
import org.bson.ByteBuf
import tech.pronghorn.coroutines.awaitable.QueueWriter
import tech.pronghorn.coroutines.core.ReadWriteConnectSelectionKeyHandler
import tech.pronghorn.mongodb.bytesbufs.ManagedByteBuf
import tech.pronghorn.mongodb.bytesbufs.PronghornByteBuf
import tech.pronghorn.plugins.internalQueue.InternalQueuePlugin
import tech.pronghorn.server.HttpServerWorker
import tech.pronghorn.server.bufferpools.OneUseByteBufferAllocator
import tech.pronghorn.server.bufferpools.ReusableBufferPoolManager
import tech.pronghorn.util.*
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel

const val MONGODB_HEADER_SIZE = 36

class MultiplexMongoDBSocket(private val factory: MultiplexMongoDBStreamFactory,
                             private val worker: HttpServerWorker,
                             private val serviceWriter: QueueWriter<MultiplexMongoDBSocket>,
                             private val socketSettings: SocketSettings,
                             val serverAddress: ServerAddress): ReadWriteConnectSelectionKeyHandler {
    private val socket = createSocket()
    private var selectionKey: SelectionKey? = null
    private val bufferPool = ReusableBufferPoolManager(kibibytes(64), worker.server.config.useDirectByteBuffers)
    private val oneUseByteBufferAllocator = OneUseByteBufferAllocator(worker.server.config.useDirectByteBuffers)
    private var readByteBuf = PronghornByteBuf(bufferPool.getBuffer())
    private val pendingHeaderRequests = InternalQueuePlugin.getUnbounded<AsyncCompletionHandler<ByteBuf>>()
    private val allPendingWriteBuffers = InternalQueuePlugin.getUnbounded<ByteBuffer>()
    private val pendingWrites = InternalQueuePlugin.getUnbounded<PendingMongoDBWrite>()
    private val openHandlers = InternalQueuePlugin.getUnbounded<AsyncCompletionHandler<Void?>>()
    private var isWriteQueued = false
    private var isReadQueued = false
    private var isConnectQueued = false
    private var isQueued = false
    private var stage: MultiplexMongoDBStatus = Idle
    private val childStreams = mutableSetOf<MultiplexMongoDBStream>()

    private fun createSocket(): SocketChannel {
        val socket = SocketChannel.open()
        socket.socket().tcpNoDelay = true
        socket.socket().keepAlive = true
        if (socketSettings.sendBufferSize > 0) {
            socket.socket().sendBufferSize = socketSettings.sendBufferSize
        }
        if (socketSettings.receiveBufferSize > 0) {
            socket.socket().receiveBufferSize = socketSettings.receiveBufferSize
        }
        socket.configureBlocking(false)
        return socket
    }

    private fun close() {
        if (!isClosed()) {
            childStreams.forEach(MultiplexMongoDBStream::close)
            socket.close()
            readByteBuf.release()
            factory.removeMultiplexer(this)
        }
    }

    internal fun removeChildStream(stream: Stream) {
        childStreams.remove(stream)
        if (childStreams.isEmpty()) {
            close()
        }
    }

    internal fun getStream(): Stream {
        val stream = MultiplexMongoDBStream(this, serverAddress)
        childStreams.add(stream)
        return stream
    }

    fun isClosed(): Boolean = socket.socket().isClosed

    fun isConnected(): Boolean = socket.socket().isConnected

    fun isConnectionPending(): Boolean = socket.isConnectionPending

    internal fun open(handler: AsyncCompletionHandler<Void?>) {
        if (isConnected()) {
            handler.completed(null)
            return
        }

        openHandlers.add(handler)

        if (!isConnectionPending()) {
            socket.connect(serverAddress.socketAddress)
            selectionKey = worker.registerSelectionKeyHandler(socket, this, SelectionKey.OP_CONNECT)
        }
    }

    private fun enqueueIfNecessary() {
        if(!isQueued) {
            isQueued = true
            serviceWriter.offer(this)
        }
    }

    override fun handleWrite() {
        selectionKey?.removeInterestOps(SelectionKey.OP_WRITE)
        isWriteQueued = true
        enqueueIfNecessary()
    }

    override fun handleRead() {
        isReadQueued = true
        enqueueIfNecessary()
    }

    override fun handleConnect() {
        selectionKey?.removeInterestOps(SelectionKey.OP_CONNECT)
        isConnectQueued = true
        enqueueIfNecessary()
    }

    internal fun process() {
        isQueued = false
        if(isReadQueued){
            processReads()
        }
        if(isWriteQueued){
            processWrites()
        }
        if(isConnectQueued){
            processConnect()
        }
    }

    private fun processConnect() {
        isConnectQueued = false
        if (!isConnected() && socket.finishConnect()) {
            var openHandler = openHandlers.poll()
            while (openHandler != null) {
                openHandler.completed(null)
                openHandler = openHandlers.poll()
            }
            selectionKey?.interestOps(SelectionKey.OP_READ)
        }
    }

    private tailrec fun processReads() {
        isReadQueued = false
        val readBuffer = readByteBuf.asNIO()
        if (readByteBuf.alreadyRead > 0 && readByteBuf.referenceCount == 1) {
            clearOrCompactReadBuffer()
        }

        val read = socket.read(readBuffer)
        if (read < 0) {
            close()
        }
        else if (read == 0 && !readBuffer.hasRemaining()) {
            if (readByteBuf.alreadyRead == 0) {
                reallocateReadByteBuf(readBuffer.capacity() * 2)
            }
            else {
                reallocateReadByteBuf(readBuffer.capacity())
            }
            processReads()
        }
        else {
            processAvailableReads()
        }
    }

    private fun processWrites() {
        val wrote = socket.write(allPendingWriteBuffers.toTypedArray())

        if (wrote == 0L) {
            selectionKey?.addInterestOps(SelectionKey.OP_WRITE)
            return
        }
        isWriteQueued = false

        var pending = pendingWrites.peek()
        while (pending != null) {
            pending.buffers.forEach { buffer ->
                if (!buffer.hasRemaining()) {
                    allPendingWriteBuffers.remove(buffer.asNIO())
                }
                else {
                    return
                }
            }
            pending.handler.completed(null)
            pendingWrites.remove()
            pending = pendingWrites.peek()
        }
    }

    internal fun addPendingWrite(buffers: List<ByteBuf>,
                                 handler: AsyncCompletionHandler<Void?>) {
        pendingWrites.add(PendingMongoDBWrite(buffers, handler))
        buffers.forEach { buffer -> allPendingWriteBuffers.add(buffer.asNIO()) }
        isWriteQueued = true
        enqueueIfNecessary()
    }

    tailrec private fun processAvailableReads() {
        val currentStage = stage
        when (currentStage) {
            Idle -> {
                val pending = pendingHeaderRequests.poll()
                if (pending != null) {
                    val slice = getSlice(MONGODB_HEADER_SIZE)
                    if (slice != null) {
                        stage = HeaderReturned
                        pending.completed(slice)
                        processAvailableReads()
                    }
                    else {
                        stage = HeaderOutstanding(pending)
                    }
                }
            }
            is HeaderOutstanding -> {
                val slice = getSlice(MONGODB_HEADER_SIZE)
                if (slice != null) {
                    stage = HeaderReturned
                    currentStage.handler.completed(slice)
                    processAvailableReads()
                }
            }
            HeaderReturned -> {
            }
            is DataOutstanding -> {
                val slice = getSlice(currentStage.numBytes)
                if (slice != null) {
                    val pending = pendingHeaderRequests.poll()
                    if (pending != null) {
                        stage = HeaderOutstanding(pending)
                    }
                    else {
                        stage = Idle
                    }
                    currentStage.handler.completed(slice)
                    processAvailableReads()
                }
            }
        }
    }

    private fun clearOrCompactReadBuffer() {
        val readBuffer = readByteBuf.asNIO()
        if (readByteBuf.position() == readByteBuf.alreadyRead) {
            readBuffer.clear()
        }
        else {
            val prePosition = readBuffer.position()
            readBuffer.position(readByteBuf.alreadyRead)
            readBuffer.compact()
            readBuffer.limit(readBuffer.capacity())
            readBuffer.position(prePosition - readByteBuf.alreadyRead)
        }
        readByteBuf.alreadyRead = 0
    }

    private fun getSlice(numBytes: Int): ByteBuf? {
        val readBuffer = readByteBuf.asNIO()
        if (readBuffer.position() - readByteBuf.alreadyRead < numBytes) {
            return null
        }

        val prePosition = readBuffer.position()
        readBuffer.position(readByteBuf.alreadyRead)
        readByteBuf.alreadyRead += numBytes
        readBuffer.limit(readByteBuf.alreadyRead)
        val sliced = readByteBuf.slice()
        readBuffer.limit(readBuffer.capacity())
        readBuffer.position(prePosition)
        return sliced
    }

    internal fun getBuffer(size: Int): ByteBuf {
        val buf = when {
            size <= bufferPool.bufferSize -> ManagedByteBuf(bufferPool.getBuffer())
            else -> ManagedByteBuf(oneUseByteBufferAllocator.getBuffer(roundToNextPowerOfTwo(size)))
        }
        buf.limit(size)
        return buf
    }

    internal fun requestHeaderRead(handler: AsyncCompletionHandler<ByteBuf>) {
        when (stage) {
            Idle -> {
                val slice = getSlice(MONGODB_HEADER_SIZE)
                if (slice != null) {
                    stage = HeaderReturned
                    handler.completed(slice)
                }
                else {
                    stage = HeaderOutstanding(handler)
                }
            }
            else -> pendingHeaderRequests.add(handler)
        }
    }

    internal fun requestDataRead(numBytes: Int,
                                 handler: AsyncCompletionHandler<ByteBuf>) {
        if (numBytes > readByteBuf.capacity()) {
            reallocateReadByteBuf(numBytes)
        }
        when (stage) {
            HeaderReturned -> {
                val slice = getSlice(numBytes)
                if (slice != null) {
                    stage = Idle
                    handler.completed(slice)
                }
                else {
                    stage = DataOutstanding(numBytes, handler)
                }
            }
            Idle -> throw Exception("Unexpected data request of $numBytes bytes without prior header request.")
            is HeaderOutstanding -> throw Exception("Unexpected data request of $numBytes bytes without prior header request completion.")
            is DataOutstanding -> throw Exception("Unexpected data request of $numBytes bytes with already outstanding body request")
        }
    }

    private fun reallocateReadByteBuf(minSize: Int) {
        val size = roundToNextPowerOfTwo(minSize)
        val newManagedBuffer = when {
            size < bufferPool.bufferSize -> bufferPool.getBuffer()
            else -> oneUseByteBufferAllocator.getBuffer(size)
        }
        val newReadByteBuf = PronghornByteBuf(newManagedBuffer)
        val newReadBuffer = newManagedBuffer.buffer

        val oldReadBuffer = readByteBuf.asNIO()
        val remainingCount = oldReadBuffer.position() - readByteBuf.alreadyRead
        if (remainingCount > 0) {
            val remainingBytes = ByteArray(remainingCount)
            oldReadBuffer.position(readByteBuf.alreadyRead)
            oldReadBuffer.get(remainingBytes)
            newReadBuffer.put(remainingBytes)
            readByteBuf.release()
        }
        readByteBuf = newReadByteBuf
    }
}
