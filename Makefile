build:
	tail -f /var/log/system.log | nc 127.0.0.1 2003
