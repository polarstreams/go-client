# Go Client for Barco Streams

Go Client for Barco Streams. Barco is a lightweight, elastic, kubernetes-native event streaming system.

## Installing

```shell
go get github.com/barcostreams/go-client
```

![go build](https://github.com/barcostreams/barco/actions/workflows/go.yml/badge.svg)

## Getting started

To start using the Go Client for Barco Streams, import the client package and set the Barco "service Url" when creating
a `Producer` or `Consumer`.

A service Url is composed by the `barco://` scheme followed by the host name or Kubernetes service name,
for example: `barco://barco.streams` refers to the service `barco` in the `streams` namespace.

### Producing messages

In order to publish records to a topic, you need to create a `Producer` instance. `Producer` instances are designed to
be long lived and thread safe, you usually need only one per application.

```go
import (
	"fmt"
	"strings"

	"github.com/barcostreams/go-client"
)

// ...

producer, err := barco.NewProducer("barco://barco.streams")
if err != nil {
	panic(err)
}

fmt.Printf("Discovered a Barco cluster with %d brokers\n", producer.BrokersLength())

topic := "my-first-topic" // The topic will be automatically created
message := strings.NewReader(`{"hello": "world"}`)
partitionKey := "" // Empty to use a random partition

if err := producer.Send(topic, message, partitionKey); err != nil {
	panic(err)
}
```

### Consuming messages

To read messages from a topic, you need to create a `Consumer` instance and set the group name that the consumer
belongs to.

When multiple consumers from a group are subscribed to a topic, each consumer in the group will receive messages
from a different set of the partitions within the topic.

`Consumer` instances are designed to be long lived. You usually need only one per application.

```go
import (
	"fmt"

	"github.com/barcostreams/go-client"
)


// ...

group := "group1"
consumer, err := barco.NewConsumer("barco://barco.streams", group, topic)
if err != nil {
	panic(err)
}

fmt.Printf("Discovered a cluster with %d brokers\n", consumer.BrokersLength())

for {
	pollResult := consumer.Poll()
	if pollResult.Error != nil {
		fmt.Printf("Found error while polling: %s", pollResult.Error)
		continue
	}

	// New records organized by topic
	for _, topicRecords := range pollResult.TopicRecords {
		for _, record := range topicRecords.Records {
			fmt.Println(string(record.Body), record.Timestamp)
		}
	}
}
```

## License
© Jorge Bay.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.