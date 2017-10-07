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
import tech.pronghorn.coroutines.awaitable.QueueWriter
import tech.pronghorn.plugins.logging.LoggingPlugin
import tech.pronghorn.server.HttpServerWorker

class MultiplexMongoDBStreamFactory(private val worker: HttpServerWorker,
                                    private val socketSettings: SocketSettings,
                                    private val sslSettings: SslSettings) : StreamFactory {
    val logger = LoggingPlugin.get(javaClass)
    val serviceWriter = getMongoDBWriteServiceWriter()

    private fun getMongoDBWriteServiceWriter(): QueueWriter<MultiplexMongoDBSocket> {
        val writeService = worker.getService<MultiplexMongoDBService>()
        if(writeService != null){
            return writeService.getQueueWriter()
        }

        if(worker.isWorkerThread()) {
            logger.debug { "Creating ${MultiplexMongoDBService::class.simpleName}, required for ${javaClass.simpleName}" }
            worker.server.addService { worker -> MultiplexMongoDBService(worker) }
        }

        return worker.getServiceQueueWriter<MultiplexMongoDBSocket, MultiplexMongoDBService>() ?:
                throw IllegalStateException("If ${javaClass.simpleName} is created outside of a worker thread, a ${MultiplexMongoDBService::class.simpleName} service must be added to the application prior to instantiation.")
    }

    private val multiplexerMap = mutableMapOf<ServerAddress, MultiplexMongoDBSocket>()

    internal fun removeMultiplexer(socket: MultiplexMongoDBSocket){
        multiplexerMap.remove(socket.serverAddress, socket)
    }

    override fun create(serverAddress: ServerAddress): Stream {
        if (sslSettings.isEnabled) {
            throw UnsupportedOperationException("SSL not supported.")
        }
        val multiplexer = multiplexerMap.getOrPut(serverAddress, {
            MultiplexMongoDBSocket(this, worker, serviceWriter, socketSettings, serverAddress)
        })

        return multiplexer.getStream()
    }
}
