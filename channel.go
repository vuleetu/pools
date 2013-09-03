package pools

import (
    "fmt"
    "sync"
    "time"
    "errors"
)

var TIME_OUT = errors.New("Time out")

type ChannelPool struct {
    ch chan byte
    sync.Mutex
    idleTimeout time.Duration
    factory Factory
    size int
    resources  chan fifoWrapper
}

func NewChannelPool(capacity int, idleTimeout time.Duration) *ChannelPool {
    return &ChannelPool{ch: make(chan byte, capacity), idleTimeout: idleTimeout, resources: make(chan fifoWrapper, capacity)}
}

func (rr *ChannelPool) Open(factory Factory) {
    rr.Lock()
    defer rr.Unlock()
    rr.factory = factory
}

func (rr *ChannelPool) Get() (Resource, error) {
    rr.ch <- 1
    rr.Lock()
    defer rr.Unlock()

    return rr.get()
}

func (rr *ChannelPool) GetWithTimeout(timeout time.Duration) (Resource, error) {
    select {
    case rr.ch <- 1:
        return rr.get()
    case <-time.After(timeout):
        fmt.Println("Time out after", timeout)
        return nil, TIME_OUT
    }
}

func (rr *ChannelPool) get() (Resource, error) {
    for {
        select {
        case fw := <-rr.resources:
            if rr.idleTimeout > 0 && fw.timeUsed.Add(rr.idleTimeout).Sub(time.Now()) < 0 {
                fmt.Println("Resource time out when get, discarded now")
                go fw.resource.Close()
                rr.size--
                continue
            } else if fw.resource.IsClosed() {
                fmt.Println("Resource is closed when get, discarded now")
                go fw.resource.Close()
                rr.size--
                continue
            }

            return fw.resource, nil
        default:
            res, err := rr.factory()
            if err != nil {
                <-rr.ch
                return nil, err
            }

            rr.size++
            return res, nil
        }
    }
}

func (rr *ChannelPool) Put(resource Resource) {
    rr.Lock()
    defer rr.Unlock()

    <-rr.ch

    if resource.IsClosed() {
        fmt.Println("Resource is closed when releasing, discarded now")
        rr.size--
        return
    }

    rr.resources <- fifoWrapper{resource, time.Now()}
}

func (rr *ChannelPool) StatsJSON() string {
	s, c, a, it := rr.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Capacity\": %v, \"Available\": %v, \"IdleTimeout\": %v}", s, c, a, int64(it))
}

func (rr *ChannelPool) Stats() (size, capacity, available int64, idleTimeout time.Duration) {
	return int64(rr.size), int64(cap(rr.resources)), int64(len(rr.resources)), rr.idleTimeout
}
