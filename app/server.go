package main

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	hostname           string
	port               string
	MASTER_REPLID      string
	MASTER_REPL_OFFSET int
	master             *Server
}

type ClientCommand struct {
	command string
	args    []string
}

type CacheValue struct {
	death int64
	val   string
}

type Client struct {
	c     net.Conn
	cache map[string]CacheValue
}

var server = Server{
	hostname: "localhost",
	port:     "6379",
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	args := os.Args
	for i, arg := range args {
		if arg == "--port" && i+1 < len(args) {
			server.port = args[i+1]
		} else if arg == "--replicaof" && i+1 < len(args) {
			master_addr := strings.Fields(args[i+1])
			masterHost, masterPort := master_addr[0], master_addr[1]
			server.master = &Server{masterHost, masterPort, "", 0, nil}
		}
	}

	if server.master == nil {
		server.MASTER_REPLID = getRandomStr(40)
		server.MASTER_REPL_OFFSET = 0
	} else {
		server.sendHandshake()
	}

	server_addr := fmt.Sprintf("localhost:%s", server.port)
	l, err := net.Listen("tcp", server_addr)
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

func (server *Server) sendHandshake() {
	masterUrl := server.master.hostname + ":" + server.master.port
	conn, err := net.Dial("tcp", masterUrl)
	if err != nil {
		fmt.Printf("Error %v. Exiting\n", err.Error())
		return
	}
	defer conn.Close()
	_, err = conn.Write(makeArray([]string{"PING"}))
	if err != nil {
		fmt.Printf("Error %v. Exiting\n", err.Error())
		return
	}
	time.Sleep(1*time.Second)
	_, err = conn.Write(makeArray([]string{"REPLCONF", "listening-port", server.port}))
	if err != nil {
		fmt.Printf("Error %v. Exiting\n", err.Error())
		return
	}
	time.Sleep(1*time.Second)
	_, err = conn.Write(makeArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		fmt.Printf("Error %v. Exiting\n", err.Error())
		return
	}
	time.Sleep(1*time.Second)
}

func handleConnection(c net.Conn) {
	cl := Client{c: c, cache: make(map[string]CacheValue)}
	defer cl.c.Close()
	for {
		input := make([]byte, 1024)
		_, err := cl.c.Read(input)
		if err != nil {
			fmt.Printf("Error %v. Exiting\n", err.Error())
			return
		}
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
		cl.handleSet(cl_cmd.args)
	case GET:
		cl.handleGet(cl_cmd.args)
	case INFO:
		cl.handleInfo(cl_cmd.args)
	case REPLCONF:
		cl.handleReplConf(cl_cmd.args)
	default:
		cl.c.Write(makeBulk("Invalid input"))
	}
}

func (cl *Client) handleInfo(args []string) {
	if len(args) != 1 || args[0] != "replication" {
		cl.c.Write(makeBulk("Invalid input\n"))
		return
	}
	if server.master == nil {
		response := fmt.Sprintf("%s:master\n", ROLE)
		response = response + fmt.Sprintf("%s:%s\n", MASTER_REPLID, server.MASTER_REPLID)
		response = response + fmt.Sprintf("%s:%d\n", MASTER_REPL_OFFSET, server.MASTER_REPL_OFFSET)
		cl.c.Write(makeBulk(response))
	} else {
		cl.c.Write(makeBulk(fmt.Sprintf("%s:slave\n", ROLE)))
	}
}

func (cl *Client) handleReplConf(args []string) {
	if len(args) < 2 {
		cl.c.Write(makeBulk("Invalid input"))
		return
	}
	cl.c.Write(makeSimple("OK"))
}

func (cl *Client) handleSet(args []string) {
	if len(args) < 2 {
		cl.c.Write(makeBulk("Invalid input"))
		return
	}
	key := args[0]
	val := args[1]
	cache_val := CacheValue{death: math.MaxInt64, val: val}
	if len(args) > 2 {
		if len(args) != 4 || strings.ToUpper(args[2]) != PX {
			cl.c.Write(makeBulk("Invalid input"))
			return
		}
		dur, _ := strconv.Atoi(args[3])
		cache_val.death = time.Now().UnixMilli() + int64(dur)
	}
	cl.cache[key] = cache_val
	cl.c.Write(makeSimple("OK"))
}

func (cl *Client) handleGet(args []string) {
	if len(args) != 1 {
		cl.c.Write(makeBulk("Invalid input"))
		return
	}
	key := args[0]
	cache_val, present := cl.cache[key]
	if present {
		if time.Now().UnixMilli() < cache_val.death {
			cl.c.Write(makeBulk(cache_val.val))
		} else {
			delete(cl.cache, key)
			cl.c.Write(makeBulkNull())
		}
	} else {
		cl.c.Write(makeBulkNull())
	}
}

func makeArray(strs []string) []byte {
	result := fmt.Sprintf("*%d\r\n", len(strs))
	for _, str := range strs {
		result = result + fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	}
	return []byte(result)
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
