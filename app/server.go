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

func handleConnection(c net.Conn) {
	defer c.Close()
	for {
		input := make([]byte, 1024)
		input_size, err := c.Read(input)
		if err != nil {
			fmt.Printf("Error %v. Exiting\n", err.Error())
			return
		}
		fmt.Printf("Recievied %v bytes from client\n", input_size)
		fmt.Printf("Length of input from client: %v\n", len(string(input)))
		cl_cmd := parseInput(string(input))
		execute(cl_cmd, c)
	}
}

type clientCommand struct {
	command string
	args    []string
}

func parseInput(command_str string) clientCommand {
	var cl_cmd clientCommand
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
			fmt.Println(cl_cmd.args)
		}
	}
	return cl_cmd
}

func execute(cl_cmd clientCommand, c net.Conn) {
	switch cl_cmd.command {
	case ECHO:
		c.Write(make_Output(cl_cmd.args[0]))
	case PING:
		c.Write(make_Output("PONG"))
	default:
		c.Write(make_Output("Invalid input"))
	}
}

func make_Output(str string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
}
