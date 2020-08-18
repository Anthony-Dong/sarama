package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
)

const (
	topic = "user_event_dev"
	group = "anthony_dev_1"
	host  = "localhost:9092,localhost:9093,localhost:9094"
)

func getAddr() []string {
	return strings.Split(host, ",")
}
func panicError(err error) {
	if err != nil {
		panic(err)
	}
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("set up ....")
	return nil
}
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("down")
	return nil
}
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("[Consumer] Message topic:%q partition:%d offset:%d add:%d\n", msg.Topic, msg.Partition, msg.Offset, claim.HighWaterMarkOffset()-msg.Offset)
	}
	return nil
}

func main() {
	go func() {
		http.ListenAndServe(":8888", http.DefaultServeMux)
	}()
	sarama.Logger = log.New(os.Stderr, "[SARAMA] ", log.LstdFlags) // 日志存储

	wg := sync.WaitGroup{}
	wg.Add(2)
	//go func() {
	//	config := newKafkaConfig()
	//	defer wg.Done()
	//	producer, err := sarama.NewSyncProducer(getAddr(), config)
	//	panicError(err)
	//	buffer := bytes.Buffer{}
	//	for {
	//		buffer.Reset()
	//		time.Sleep(time.Millisecond * 100)
	//		buffer.WriteString(fmt.Sprintf("curent: %v", time.Now().UnixNano()))
	//		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
	//			Topic: topic,
	//			Value: sarama.ByteEncoder(buffer.Bytes()),
	//		})
	//		panicError(err)
	//		fmt.Fprintf(ioutil.Discard, "[Producer] partition: %v, offset: %v, topic: %v\n", partition, offset, topic)
	//	}
	//}()

	go func() {
		config := newKafkaConfig()
		defer wg.Done()
		client, err := sarama.NewConsumerGroup(getAddr(), group, config)
		panicError(err)
		ctx := context.TODO()
		go func() {
			select {
			case <-ctx.Done():
				fmt.Println("consumer down")
			}
		}()
		handler := new(exampleConsumerGroupHandler)
		err = client.Consume(ctx, []string{topic}, handler)
		panicError(err)

	}()

	wg.Wait()
}

func newKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	// 全局一个config即可
	config.ClientID = "sarama_demo"
	config.Version = sarama.V0_11_0_1
	// kafka版本号
	config.Producer.Return.Successes = true
	// 同步必须开启
	config.Producer.RequiredAcks = sarama.WaitForAll
	// ack=-1
	config.Metadata.Full = false
	// 不需要拉取全部配置
	config.Consumer.Offsets.AutoCommit.Enable = true
	// 自动提交
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Defaults to OffsetNewest. 所以这个比较坑，一开始对于一个没有启动过的分支，最好设置一下
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	// rb策略，default BalanceStrategyRange
	return config
}
