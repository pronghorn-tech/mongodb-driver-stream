# Pronghorn MongoDB Driver Stream
This library implements the Stream interface of the [MongoDB Async Driver](https://github.com/mongodb/mongo-java-driver/tree/master/driver-async) via the [Pronghorn Coroutine Framework](https://github.com/pronghorn-tech/coroutines). It utilizes the cooperative nature of concurrency in Pronghorn to enable efficient multiplexing of database communication, yielding significant performance benefits.

## Quick Start
Utilizing this library within a Pronghorn based application simply requires creating an async MongoDB client via the provided factory implementation.

```kotlin
val multiplexedFactory = MultiplexMongoDBStreamFactoryFactory(worker)
val settings = MongoClientSettings.builder()
                .streamFactoryFactory(multiplexedFactory)
                ...[other settings]
                .build()

        val client = MongoClients.create(settings)
```

Since the MultiplexMongoDBStreamFactoryFactory constructor takes a worker argument, a separate client should be created per worker.

# License
Copyright 2017 Pronghorn Technology LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
