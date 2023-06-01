### 概述
这个Go程序用于消费Kafka消息并将满足指定规则的消息写入Elasticsearch进行存储和索引。它提供了灵活的规则匹配和消息过滤功能。

### 依赖
确保您已经安装了以下依赖：

Go语言环境
github.com/confluentinc/confluent-kafka-go/v2/kafka库
github.com/elastic/go-elasticsearch/v7库
您可以使用以下命令通过Go模块管理器下载依赖：

go
Copy code
go mod tidy
### 配置
在程序的main函数中，您需要进行以下配置：

kfk_broker：Kafka服务器的地址。
group：消费者组ID。
topicRules：要订阅的主题和相应的规则。
确保将Addresses字段的值设置为Elasticsearch的地址：

vbnet
Copy code
Addresses: []string{"http://xxx:9200"},
### 使用
运行程序：

go
Copy code
go run main.go
程序将会创建一个Kafka消费者，并根据配置的规则从相应的主题中消费消息。满足规则的消息将会被过滤并写入到Elasticsearch中进行索引。

确保在运行程序之前，Kafka服务器和Elasticsearch服务器都已启动，并且您具有相应的权限和访问凭证。

### 注意事项
在ElasticWrite方法中，数据将会立即写入Elasticsearch。如果需要更高的性能，建议使用批量写入方法。
该程序使用了并发消费，每个分区都会启动一个独立的消费者。您可以根据需要进行调整。
在程序中使用了硬编码的地址和配置值，请根据实际情况进行修改。
这是一个基本的README示例，您可以根据自己的需求进行扩展和修改。确保提供足够的说明，以便其他人能够理解和正确使用该程序。
