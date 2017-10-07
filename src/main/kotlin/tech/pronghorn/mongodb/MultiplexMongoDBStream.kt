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
import com.mongodb.connection.AsyncCompletionHandler
import com.mongodb.connection.Stream
import org.bson.ByteBuf
import java.nio.channels.SocketChannel

class MultiplexMongoDBStream(private val multiplexSocket: MultiplexMongoDBSocket,
                             private val serverAddress: ServerAddress) : Stream {
    private val blockingSocket = SocketChannel.open()
    private var isClosed = false
    private var hasOutstandingHeaderRequest = false

    override fun close() {
        multiplexSocket.removeChildStream(this)

        if(isClosed){
            return
        }

        if(blockingSocket.isConnected){
            blockingSocket.close()
        }
    }

    override fun isClosed(): Boolean = isClosed

    override fun getAddress(): ServerAddress = serverAddress

    override fun getBuffer(size: Int): ByteBuf = multiplexSocket.getBuffer(size)

    override fun open() {
        blockingSocket.connect(serverAddress.socketAddress)
    }

    override fun openAsync(handler: AsyncCompletionHandler<Void?>) {
        multiplexSocket.open(handler)
    }

    override fun write(buffers: List<ByteBuf>) {
        if(!blockingSocket.isConnected) {
            open()
        }
        blockingSocket.write(buffers.map { it.asNIO() }.toTypedArray())
    }

    override fun writeAsync(buffers: List<ByteBuf>,
                            handler: AsyncCompletionHandler<Void?>) {
        if(!multiplexSocket.isConnected()){
            openAsync(ForwardedAsyncCompletionHandler {
                writeAsync(buffers, handler)
            })
        }
        else {
            multiplexSocket.addPendingWrite(buffers, handler)
        }
    }

    override fun read(numBytes: Int): ByteBuf {
        if(!blockingSocket.isConnected) {
            open()
        }
        val buffer = getBuffer(numBytes)
        var totalRead = 0
        while (totalRead < numBytes) {
            totalRead += blockingSocket.read(buffer.asNIO())
        }
        buffer.flip()
        return buffer
    }

    override fun readAsync(numBytes: Int,
                           handler: AsyncCompletionHandler<ByteBuf>) {
        if(!multiplexSocket.isConnected()){
            openAsync(ForwardedAsyncCompletionHandler {
                readAsync(numBytes, handler)
            })
        }
        else {
            if (numBytes == MONGODB_HEADER_SIZE && !hasOutstandingHeaderRequest) {
                hasOutstandingHeaderRequest = true
                multiplexSocket.requestHeaderRead(handler)
            }
            else {
                hasOutstandingHeaderRequest = false
                multiplexSocket.requestDataRead(numBytes, handler)
            }
        }
    }
}

class ForwardedAsyncCompletionHandler<T>(private val block: () -> Unit): AsyncCompletionHandler<T> {
    override fun completed(t: T) {
        block()
    }

    override fun failed(ex: Throwable) {
        throw ex
    }

}
