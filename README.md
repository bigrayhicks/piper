# Piper

This is an example on how to stream data via AWS kinesis with golang

## Install

1) Create an AWS kinesis stream

Change the constant StreamName in `piper.go` 

2) start the receiver

cd cmd/preciever/ && make

3) start the sender

cd cmd/ppusher/ && make

4) Use telnet to send to the ppuser

telnet 127.0.0.1 2003

```
$ telnet 127.0.0.1 2003
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
hello!
```

5. check the ppusher and the preciever


