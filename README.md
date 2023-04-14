
## 主要組成
* topic：一个 topic 就是程序发布消息的一个逻辑键，当程序第一次发布消息时就会创建 topic。
* channel：channel 与消费者相关，每当一个发布者发送一条消息到一个 topic，消息会被复制到 topic 下面所有的 channel 上，消费者通过 channel 读取消息。同一个 channel 可以由多个消费者同时连接，以达到负载均衡的效果。
* message：s消息是数据流的抽象，消费者可以选择结束消息，表明它们已被正常处理，或者重新将它们排队待到后面再进行处理。
* smqd：smqd 是一个守护进程，负责接收，排队，投递消息给客户端。


## channel
* 维护消费者信息
* 收发消息
> * msgChan：这是一个有缓冲管道，用来暂存消息，超过长度则丢弃消息（后续会加上持久化到磁盘的功能）
> * incomingMessageChan：用来接收生产者的消息
> * clientMessageChan：消息会被发送到这个管道，后续会由消费者拉取
* 关闭
* 重入队列


## topic
topic 的作用是接收客户端的消息，然后同时发送给所有绑定的 channel 上
* name：名称
* newChannelChan：新增 channel 的管道
* channelMap：维护的 channel 集合
* incomingMessageChan：接收消息的管道
* msgChan：有缓冲管道，相当于消息的内存队列
* readSyncChan：和 routerSyncChan 配合使用保证 channelMap 的并发安全
* routerSyncChan：见上
* exitChan：接收退出信号的管道
* channelWriteStarted：是否已向 channel 发送消息


## Source
[SQM](https://github.com/yhao1206/SMQ)