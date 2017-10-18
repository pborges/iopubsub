package main

import (
	"github.com/pborges/iopubsub"
	"time"
	"encoding/json"
	"fmt"
	"bufio"
	"os"
)

type MessageOne struct {
	Now time.Time
}

func main() {
	b := new(iopubsub.Broker)

	go func() {
		topic := "topic0"
		p1 := json.NewEncoder(b.Publish(topic, 512))
		for {
			time.Sleep(1 * time.Second)
			fmt.Println("Publish to", topic)
			p1.Encode(MessageOne{Now: time.Now()})
		}
	}()

	go func() {
		topic := "topic1"
		p1 := json.NewEncoder(b.Publish(topic, 512))
		for {
			time.Sleep(5 * time.Second)
			fmt.Println("Publish to", topic)
			p1.Encode(MessageOne{Now: time.Now()})
		}
	}()

	for i := 0; i < 10; i++ {
		go func(i int) {
			topic := fmt.Sprintf("topic%d", i%2)
			fmt.Printf("Worker #%d subscribing to %s\n", i, topic)
			r1 := json.NewDecoder(b.Subscribe(topic))
			m := MessageOne{}
			for {
				if err := r1.Decode(&m); err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("worker: #%d topic: %s msg: %s\n", i, topic, m.Now)
			}
		}(i)
	}

	bufio.NewReader(os.Stdin).ReadLine()
}
