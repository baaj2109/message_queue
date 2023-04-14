package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Msg struct {
	Id       int64
	TopicLen int64
	Topic    string

	/*
		1 - consumer
		2 - producer
		3 - comsumer-acl
		4- error
	*/
	MsgType int64  // 消息類型
	Len     int64  // 消息長度
	Payload []byte //消息
}

func BytesToMsg(reader io.Reader) Msg {
	m := Msg{}
	var buf [128]byte
	n, err := reader.Read(buf[:])
	if err != nil {
		fmt.Println("failed to read, err:", err)
	}
	fmt.Println("read bytes: ", n)

	buff := bytes.NewBuffer(buf[0:8])
	binary.Read(buff, binary.LittleEndian, &m.Id)

	buff = bytes.NewBuffer(buf[8:16])
	binary.Read(buff, binary.LittleEndian, &m.TopicLen)

	msgLastIndex := 16 + m.TopicLen
	m.Topic = string(buf[16:msgLastIndex])
	buff = bytes.NewBuffer(buf[msgLastIndex : msgLastIndex+8])
	binary.Read(buff, binary.LittleEndian, &m.MsgType)

	buff = bytes.NewBuffer(buf[msgLastIndex : msgLastIndex+16])
	binary.Read(buff, binary.LittleEndian, &m.Len)

	if m.Len <= 0 {
		return m
	}

	m.Payload = buf[msgLastIndex+16:]
	return m
}

func MsgToBytes(msg Msg) []byte {
	msg.TopicLen = int64(len([]byte(msg.Topic)))
	msg.Len = int64(len([]byte(msg.Payload)))

	var data []byte
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.Id)
	data = append(data, buf.Bytes()...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.TopicLen)
	data = append(data, buf.Bytes()...)

	data = append(data, []byte(msg.Topic)...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.MsgType)
	data = append(data, buf.Bytes()...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.Len)
	data = append(data, buf.Bytes()...)
	data = append(data, []byte(msg.Payload)...)

	return data
}
