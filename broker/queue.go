package broker

import (
	"container/list"
	"sync"
)

type Queue struct {
	len  int
	data list.List
	lock sync.Mutex
}

func (q *Queue) offer(msg Msg) {
	q.data.PushBack(msg)
	q.len = q.data.Len()
}

func (q *Queue) poll() Msg {
	if q.len == 0 {
		return Msg{}
	}
	msg := q.data.Front()
	return msg.Value.(Msg)
}

func (q *Queue) delete(id int64) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for msg := q.data.Front(); msg != nil; msg = msg.Next() {
		if msg.Value.(Msg).Id == id {
			q.data.Remove(msg)
			q.len = q.data.Len()
			break
		}
	}
}
