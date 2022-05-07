package internal

import "sync/atomic"

type BrokerStatusInfo struct {
	isUpFlag       int32
	isReconnecting int32
}

func NewBrokerStatusInfo() *BrokerStatusInfo {
	return &BrokerStatusInfo{
		isUpFlag: 1,
	}
}

func (b *BrokerStatusInfo) IsUp() bool {
	return atomic.LoadInt32(&b.isUpFlag) == 1
}

func (b *BrokerStatusInfo) SetAsUp() {
	atomic.StoreInt32(&b.isUpFlag, 1)
}

func (b *BrokerStatusInfo) SetAsDown() {
	atomic.StoreInt32(&b.isUpFlag, 0)
}

func (b *BrokerStatusInfo) SetAsReconnecting() bool {
	return atomic.CompareAndSwapInt32(&b.isReconnecting, 0, 1)
}

func (b *BrokerStatusInfo) SetAsReconnected() {
	atomic.StoreInt32(&b.isReconnecting, 0)
}
