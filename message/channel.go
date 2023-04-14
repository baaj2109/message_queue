package message

import (
	"errors"
	"log"
	util "message_queue/utils"
	"time"
)

/*
消费者是从 channel 中读取消息的，
所以 channel 中需要维护消费者的信息，并且可以增删消费者
*/
type Consumer interface {
	Close()
}

type Channel struct {
	name                string
	addClientChan       chan util.ChanReq
	removeClientChan    chan util.ChanReq
	clients             []Consumer
	incomingMessageChan chan *Message     //用來接收生產者消息
	msgChan             chan *Message     //用來暫存消息 超過長度則丟棄消息
	clientMessageChan   chan *Message     //消息會被發送到這個channel 後續會由消費者垃曲
	exitChan            chan util.ChanReq //接收到信號時關閉發送消息的 message pump 協程和消費者連接

	// 三种的质量保证：
	// At most once: 至多一次。消息在传递时，最多会被送达一次。换一个说法就是，没什么消息可靠性保证，允许丢消息。
	// At least once: 至少一次。消息在传递时，至少会被送达一次。也就是说，不允许丢消息，但是允许有少量重复消息出现。
	// Exactly once：恰好一次。消息在传递时，只会被送达一次，不允许丢失也不允许重复。
	// 大部分消息队列系统提供的都是 At least once 语义， 然后用幂等性来保证业务的正确性。
	// 而 At least once 语义最常见的实现方式就是接收方在接收到消息后进行回复确认，
	// 类似 TCP 握手中的 ACK
	// 要在 channel 中维护一个 map 用于存储已经发送的消息，以及一个管道来辅助写入
	inFlightMessageChan chan *Message
	inFlightMessages    map[string]*Message
	finishMessageChan   chan util.ChanReq

	// 消息重新入隊 消費者想多次消費同一條消息
	requeueMessageChan chan util.ChanReq
}

func NewChannel(name string, inMemSize int) *Channel {
	channel := &Channel{
		name:                name,
		addClientChan:       make(chan util.ChanReq),
		removeClientChan:    make(chan util.ChanReq),
		clients:             make([]Consumer, 0, 5),
		incomingMessageChan: make(chan *Message, 5),
		msgChan:             make(chan *Message, inMemSize),
		clientMessageChan:   make(chan *Message),
		exitChan:            make(chan util.ChanReq),
		inFlightMessageChan: make(chan *Message),
		inFlightMessages:    make(map[string]*Message),
		requeueMessageChan:  make(chan util.ChanReq),
		finishMessageChan:   make(chan util.ChanReq),
		// backend:             queue.NewDiskQueue(name),
	}
	go channel.Router()
	return channel
}

func (c *Channel) AddClient(client Consumer) {
	log.Printf("Channel(%s): adding client...", c.name)
	doneChan := make(chan interface{})
	c.addClientChan <- util.ChanReq{
		Variable: client,
		RetChan:  doneChan,
	}
	<-doneChan
}

func (c *Channel) RemoveClient(client Consumer) {
	log.Printf("Channel(%s): removing client...", c.name)
	doneChan := make(chan interface{})
	c.removeClientChan <- util.ChanReq{
		Variable: client,
		RetChan:  doneChan,
	}
	<-doneChan
}

func (c *Channel) pushInFlightMessage(msg *Message) {
	c.inFlightMessages[util.UuidToStr(msg.Uuid())] = msg
}

func (c *Channel) popInFlightMessage(uuidStr string) (*Message, error) {
	msg, ok := c.inFlightMessages[uuidStr]
	if !ok {
		return nil, errors.New("UUID not in flight")
	}
	delete(c.inFlightMessages, uuidStr)
	msg.EndTimer()
	return msg, nil
}

func (c *Channel) FinishMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.finishMessageChan <- util.ChanReq{
		Variable: uuidStr,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

func (c *Channel) RequeueMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.requeueMessageChan <- util.ChanReq{
		Variable: uuidStr,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

// 在发送消息的时候，我们也往 inFlightMessageChan 中写入，
// 同时在事件处理函数 Router 中增加对该管道的监听，
// 将接收到的消息添加到 inFlightMessages 中
func (c *Channel) RequeueRouter(closeChan chan struct{}) {
	for {
		select {
		case msg := <-c.inFlightMessageChan:
			c.pushInFlightMessage(msg)
			// 如果消费者迟迟不确认完成的话，消息就会大量堆积在 inFlightMessages 中。
			// 我们可以添加这样一个逻辑：在限定的时间内如果消息没有确认完成的话，
			// 我们就将该消息自动重新入队
			go func(msg *Message) {
				select {
				case <-time.After(60 * time.Second):
					log.Printf("CHANNEL(%s): auto requeue of message(%s)", c.name, util.UuidToStr(msg.Uuid()))
				case <-msg.timerChan:
					return
				}
				err := c.RequeueMessage(util.UuidToStr(msg.Uuid()))
				if err != nil {
					log.Printf("ERROR: channel(%s) - %s", c.name, err.Error())
				}
			}(msg)

		case finishReq := <-c.finishMessageChan:
			uuidStr := finishReq.Variable.(string)
			_, err := c.popInFlightMessage(uuidStr)
			if err != nil {
				log.Printf("ERROR: failed to finish message(%s) - %s", uuidStr, err.Error())
			}
			finishReq.RetChan <- err

		case requeueReq := <-c.requeueMessageChan:
			uuidStr := requeueReq.Variable.(string)
			msg, err := c.popInFlightMessage(uuidStr)
			if err != nil {
				log.Printf("ERROR: failed to requeue message(%s) - %s", uuidStr, err.Error())
			} else {
				go func(msg *Message) {
					c.PutMessage(msg)
				}(msg)
			}
			requeueReq.RetChan <- err

		case <-closeChan:
			return
		}
	}
}

// Router handles the events of Channel
func (c *Channel) Router() {
	var (
		clientReq util.ChanReq
		closeChan = make(chan struct{})
	)
	go c.RequeueRouter(closeChan)
	go c.MessagePump(closeChan)

	for {
		select {
		case clientReq = <-c.addClientChan:
			client := clientReq.Variable.(Consumer)
			c.clients = append(c.clients, client)
			log.Printf("CHANNEL(%s) added client %#v", c.name, client)
			clientReq.RetChan <- struct{}{}

		case clientReq = <-c.removeClientChan:
			client := clientReq.Variable.(Consumer)
			indexToRemove := -1
			for k, v := range c.clients {
				if v == client {
					indexToRemove = k
					break
				}
			}
			if indexToRemove == -1 {
				log.Printf("ERROR: could not find client(%#v) in clients(%#v)", client, c.clients)
			} else {
				c.clients = append(c.clients[:indexToRemove], c.clients[indexToRemove+1:]...)
				log.Printf("CHANNEL(%s) removed client %#v", c.name, client)
			}
			clientReq.RetChan <- struct{}{}

		case msg := <-c.incomingMessageChan:
			// 防止因 msgChan 缓冲填满时造成阻塞，加上一个 default 分支直接丢弃消息
			select {
			case c.msgChan <- msg:
				log.Printf("CHANNEL(%s) wrote message", c.name)
			default:
			}

		case closeReq := <-c.exitChan:
			log.Printf("CHANNEL(%s) is closing", c.name)
			close(closeChan)

			for _, consumer := range c.clients {
				consumer.Close()
			}

			closeReq.RetChan <- nil
		}
	}
}

func (c *Channel) PutMessage(msg *Message) {
	c.incomingMessageChan <- msg
}

func (c *Channel) PullMessage() *Message {
	return <-c.clientMessageChan
}

// MessagePump send messages to ClientMessageChan
func (c *Channel) MessagePump(closeChan chan struct{}) {
	var msg *Message

	for {
		select {
		case msg = <-c.msgChan:
		case <-closeChan:
			return
		}
		if msg != nil {
			c.inFlightMessageChan <- msg
		}

		c.clientMessageChan <- msg
	}
}

func (c *Channel) Close() error {
	errChan := make(chan interface{})
	c.exitChan <- util.ChanReq{
		RetChan: errChan,
	}

	err, _ := (<-errChan).(error)
	return err
}
