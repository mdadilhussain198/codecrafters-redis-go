package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
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

type ClientCommand struct {
	command string
	args    []string
}

type Client struct {
	c     net.Conn
	cache map[string]string
}

func handleConnection(c net.Conn) {
	cl := Client{c: c, cache: make(map[string]string)}
	defer cl.c.Close()
	for {
		input := make([]byte, 1024)
		input_size, err := cl.c.Read(input)
		if err != nil {
			fmt.Printf("Error %v. Exiting\n", err.Error())
			return
		}
		fmt.Printf("Recievied %v bytes from client\n", input_size)
		cl_cmd := parseInput(string(input))
		cl.execute(cl_cmd)
	}
}

func parseInput(command_str string) ClientCommand {
	var cl_cmd ClientCommand
	parser := Parser(command_str)
	data, err := parser.getNext()
	if err != nil || data.dataType != RESP_ARRAY {
		cl_cmd.command = "INVALID"
		cl_cmd.args = make([]string, 0)
		return cl_cmd
	}
	args_count, err := strconv.Atoi(data.value)
	args_count--
	if err != nil {
		cl_cmd.command = "INVALID"
		cl_cmd.args = make([]string, 0)
		return cl_cmd
	}
	fmt.Println("Args count: ", args_count)

	for ; args_count+1 > 0 && parser.hasNext(); args_count-- {
		data, err := parser.getNext()
		if err != nil || data.dataType != RESP_BULK {
			cl_cmd.command = "INVALID"
			cl_cmd.args = make([]string, 0)
			return cl_cmd
		}
		if len(cl_cmd.command) == 0 {
			cl_cmd.command = strings.ToUpper(data.value)
		} else {
			cl_cmd.args = append(cl_cmd.args, data.value)
		}
	}
	return cl_cmd
}

func (cl *Client) execute(cl_cmd ClientCommand) {
	switch cl_cmd.command {
	case ECHO:
		if len(cl_cmd.args) < 1 {
			cl.c.Write(makeBulk("Invalid input"))
			break
		}
		cl.c.Write(makeBulk(cl_cmd.args[0]))
	case PING:
		cl.c.Write(makeBulk("PONG"))
	case SET:
		if len(cl_cmd.args) < 2 {
			cl.c.Write(makeBulk("Invalid input"))
			break
		}
		cl.handleSet(cl_cmd.args[0], cl_cmd.args[1])
	case GET:
		if len(cl_cmd.args) < 1 {
			cl.c.Write(makeBulk("Invalid input"))
			break
		}
		cl.handleGet(cl_cmd.args[0])
	default:
		cl.c.Write(makeBulk("Invalid input"))
	}
}

func (cl *Client) handleSet(key string, val string) {
	cl.cache[key] = val
	cl.c.Write(makeSimple("OK"))
}

func (cl *Client) handleGet(key string) {
	val, present := cl.cache[key]
	if present {
		cl.c.Write(makeBulk(val))
	} else {
		cl.c.Write(makeBulkNull())
	}
}

func makeBulk(str string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
}

func makeBulkNull() []byte {
	return []byte("$-1\r\n")
}

func makeSimple(str string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", str))
}
