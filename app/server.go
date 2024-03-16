package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			c.Close()
			return
		}

		go handleConnection(c)
	}
}

func handleConnection(c net.Conn) {
	defer c.Close()
	for {
		input := make([]byte, 20)
		n, err := c.Read(input)
		if err != nil {
			fmt.Printf("Error %v. Exiting\n", err.Error())
			return
		}
		fmt.Printf("Recievied %v bytes from client\n", n)
		c.Write([]byte("+PONG\r\n"))
	}
}
