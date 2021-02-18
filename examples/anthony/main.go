package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	topic    = "Test-Topic-2"
	group    = "dev-1"
	host     = "localhost:9191,localhost:9192,localhost:9193"
	clientId = "GoLang"
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
		fmt.Printf("[Consumer] Message topic:%q partition:%d offset:%d HighWaterMarkOffset:%d\n", msg.Topic, msg.Partition, msg.Offset, claim.HighWaterMarkOffset())
		sess.MarkMessage(msg, "")
		//sess.Commit()
	}
	return nil
}

func Main() {
	fmt.Println(newByte(111))
}
func main() {
	go func() {
		http.ListenAndServe(":8888", http.DefaultServeMux)
	}()
	sarama.Logger = log.New(os.Stderr, "[SARAMA] ", log.LstdFlags) // 日志存储

	wg := sync.WaitGroup{}
	wg.Add(2)
	producer(&wg)

	consumer(&wg)

	wg.Wait()
}

func producer(wg *sync.WaitGroup) {
	go func() {
		config := newKafkaConfig()
		defer wg.Done()
		producer, err := sarama.NewSyncProducer(getAddr(), config)
		panicError(err)
		buffer := bytes.Buffer{}
		for {
			buffer.Reset()
			buffer.WriteString(fmt.Sprintf("%v%s", time.Now().UnixNano(), newByte(100)))
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(buffer.Bytes()),
			})
			if err != nil {
				log.Printf("producer find err: %v", err)
			}
			fmt.Printf("[Producer] partition: %v, offset: %v, topic: %v\n", partition, offset, topic)
			time.Sleep(time.Millisecond * 100)
		}
	}()
}

func newByte(num int) string {
	builder := strings.Builder{}
	for x := 0; x < num; x++ {
		n := rand.Int31n(90-65) + 65
		builder.WriteByte(byte(n))
	}
	return builder.String()
}

func consumer(wg *sync.WaitGroup) {
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
		for {
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("consumer find error: %v\n", err)
			}
			if ctx.Err() != nil {
				log.Fatalf("consumer is down: %v\n", ctx.Err())
				return
			}
		}

	}()
}

func newKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	// 全局一个config即可
	config.ClientID = clientId
	// kafka版本号
	config.Version = sarama.V2_1_0_0
	// 同步必须开启
	config.Producer.Return.Successes = true
	// ack=-1
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 不需要拉取全部配置
	config.Metadata.Full = false
	// 自动提交
	config.Consumer.Offsets.AutoCommit.Enable = true
	// Defaults to OffsetNewest. 所以这个比较坑，一开始对于一个没有启动过的分支，最好设置一下
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	// rb策略，default BalanceStrategyRange
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	return config
}
