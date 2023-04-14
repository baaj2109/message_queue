
package server

import (
    "io"
)

type FakeConn struct {
    io.ReadWriter
}

func (c *FakeConn) Close() error {
    return nil
}

----------

protocol/protocol.go

func (p *Protocol) PUB(client StatefulReadWriter, params []string) ([]byte, error) {
    var buf bytes.Buffer
    var err error

    //  fake clients don't get to ClientInit
    if client.GetState() != -1 {
        return nil, ClientErrInvalid
    }

    if len(params) < 3 {
        return nil, ClientErrInvalid
    }

    topicName := params[1]
    body := []byte(params[2])

    _, err = buf.Write(<-util.UuidChan)
    if err != nil {
        return nil, err
    }

    _, err = buf.Write(body)
    if err != nil {
        return nil, err
    }

    topic := message.GetTopic(topicName)
    topic.PutMessage(message.NewMessage(buf.Bytes()))

    return []byte("OK"), nil
}
