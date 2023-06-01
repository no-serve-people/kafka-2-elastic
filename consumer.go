package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type TopicRules struct {
	Topic string
	Rules [][]string
}

type Consumer struct {
	GroupID    string
	Brokers    string
	TopicRules []TopicRules
	Esclient   *elasticsearch.Client
}

type FilterData struct {
	Rule      string    `json:"rule"`
	Timestamp time.Time `json:"@timestamp"`
	Msg       string    `json:"msg"`
	Topic     string    `json:"topic"`
}

func (c *Consumer) ElasticWrite(f *FilterData) {
	var now = time.Now()
	curdate := now.Format("2006.01.02")
  
        // @fixme 极致性能 这个可以使用 bulk 方法 批量发送
	res, err := c.Esclient.Index("go-monitor-index-"+curdate, esutil.NewJSONReader(&f))
	if err != nil {
		log.Printf("插入ES错误：%v ,插入数据为：%v", err, *f)
	}

	defer res.Body.Close()
        // 不读取 不会归还到idleConn
	io.ReadAll(res.Body)
}

func NewConsumer(topicRules []TopicRules, groupID, brokers string) *Consumer {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://xxx:9200"},
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 200, // 设置每个主机的最大闲置连接数
			MaxIdleConns:        300, // 设置总的最大闲置连接数 不传默认值100
			//MaxConnsPerHost:     0,   // 设置每个主机的最大连接数，0 表示不限制
		},
	}

	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("ES Writer 客户端创建失败: %s", err)
	}
	return &Consumer{
		TopicRules: topicRules,
		GroupID:    groupID,
		Brokers:    brokers,
		Esclient:   esClient,
	}
}

func (c *Consumer) handleData() {

	// Kafka 消费者配置
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": c.Brokers, // Kafka 服务器地址
		"group.id":          c.GroupID, // 消费者组 ID
		"auto.offset.reset": "latest",  // 初始消费位置，earliest 为从最早的消息开始消费
	}

	// 创建 Kafka 消费者
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		panic(err)
	}

	for _, topicRule := range c.TopicRules {
		// 获取指定 topic 下的所有分区
		metadata, err := consumer.GetMetadata(&topicRule.Topic, false, 5000)
		if err != nil {
			panic(err)
		}

		for _, partition := range metadata.Topics[topicRule.Topic].Partitions {
			// 分配当前分区
			err = consumer.Assign([]kafka.TopicPartition{{Topic: &topicRule.Topic, Partition: partition.ID}})
			if err != nil {
				panic(err)
			}

			// 单独启动每个分区消费者的消费循环
			go func(tc *kafka.Consumer, tprule TopicRules) {
				for {
					ev := tc.Poll(0)
					if ev == nil {
						continue
					}

					switch e := ev.(type) {
					case *kafka.Message:
						msg := make(map[string]interface{})
						err := json.Unmarshal(e.Value, &msg)
						if err != nil {
							continue
						}

						for _, rule := range tprule.Rules {
							var flag = true
							for _, r := range rule {
								if !strings.Contains(msg["message"].(string), r) {
									flag = false
									break
								}
							}

							if flag {
								// 时间戳转换
								t := msg["@timestamp"].(string)
								t2, _ := time.Parse("2006-01-02T15:04:05.000Z", t)

								d := &FilterData{
									Rule:      fmt.Sprintf("%v", strings.Join(rule, ",")),
									Topic:     tprule.Topic,
									Timestamp: t2,
									Msg:       msg["message"].(string),
								}
								c.ElasticWrite(d)
							}
						}

						//fmt.Printf("Received message from partition %d: %s\n", e.TopicPartition.Partition, string(e.Value))
					case kafka.Error:
						log.Printf("Error: %v\n", e)
					}
				}
			}(consumer, topicRule)

		}
	}

	// 阻塞主线程
	select {}
}

func main() {
	var kfk_broker = "xxxxxxx"
	group := xxx-goland"
	var topicRules = []TopicRules{{Topic: "aaa", Rules: [][]string{{"接口调用失败"}}}, {Topic: "bbb", Rules: [][]string{{"ERROR"}}}}

	NewConsumer(topicRules, group, kfk_broker).handleData()
}
