package iopubsub

import (
	"io"
	"sync"
	"errors"
	"log"
)

type Broker struct {
	lock        sync.Mutex
	publishers  map[string]*io.PipeReader
	subscribers map[string][]*io.PipeWriter
}

func (b *Broker) Subscribe(topic string) io.ReadCloser {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.subscribers == nil {
		b.subscribers = make(map[string][]*io.PipeWriter)
	}
	if _, ok := b.subscribers[topic]; !ok {
		b.subscribers[topic] = make([]*io.PipeWriter, 0)
	}
	pr, pw := io.Pipe()
	b.subscribers[topic] = append(b.subscribers[topic], pw)
	return pr
}

func (b *Broker) Publish(topic string, bufferSize int) io.WriteCloser {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.publishers == nil {
		b.publishers = make(map[string]*io.PipeReader)
	}
	pr, pw := io.Pipe()
	b.publishers[topic] = pr
	go func() {
		buf := make([]byte, bufferSize)
		closing := false
		for {
			n, err := pr.Read(buf)
			if err != nil {
				log.Printf("unable to read from topic %s error: %+v", topic, err)
				closing = true
			}
			if _, ok := b.subscribers[topic]; ok {
				b.lock.Lock()
				subscribers := make([]*io.PipeWriter, 0)
				for _, s := range b.subscribers[topic] {
					if closing {
						s.CloseWithError(errors.New("publisher is closed"))
						continue
					}
					_, err = s.Write(buf[:n])
					if err == nil {
						subscribers = append(subscribers, s)
					} else {
						log.Printf("unable to write to subscriber from topic %s error: %+v", topic, err)
					}
				}
				b.subscribers[topic] = subscribers
				b.lock.Unlock()
			}
			if closing {
				return
			}
		}
	}()
	return pw
}
