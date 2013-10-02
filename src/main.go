/**
 * Created with IntelliJ IDEA.
 * User: jhaddad
 * Date: 10/1/13
 * Time: 10:26 PM
 * To change this template use File | Settings | File Templates.
 */
package main

import "fmt"
import (
	"net"
	"strings"
)

type Request struct {
	Action string
	Key string
	Value string
	Response string
}

func NewRequest(command string) Request {
	// trim the string
	command = strings.TrimSpace(command)
	args := strings.Split(command, " ")
	if len(args) == 2 {
		args = append(args, "")
	}
	return Request{args[0], args[1], args[2], ""}
}

func main() {
	fmt.Println("Hello world!")

	fmt.Println("Allocating channel")

	dm_chan := make(chan Request)

	go data_manager(dm_chan)

	fmt.Println("creating map")

	fmt.Println("starting server")

	sock, err := net.Listen("tcp", ":7789")

	if err != nil {
		fmt.Println("could not start")
		panic("port is fucked")
	}

	fmt.Println("listening")

	for {
		fmt.Println("Waiting for connection")
		conn, err := sock.Accept()

		if err != nil {
			fmt.Println("fail")
			continue
		}
		go handle_connection(conn, dm_chan)

	}
}

// manage the map
func data_manager(dm_chan chan Request) {
	data := make(map[string]string)

	for {
		req := <-dm_chan
		if req.Action == "set" {
			data[req.Key] = req.Value
			req.Response = req.Key
		} else if req.Action == "get" {
			fmt.Println("get request:", req.Key)
			fmt.Println(req)
			req.Response = data[req.Key]
		} else if req.Action == "delete" {
			delete(data, req.Key)
			req.Response = "OK"
		} else {
			req.Response = "UNKNOWN"
		}
		dm_chan <- req
		fmt.Println(data)

	}
}

func handle_connection(conn net.Conn, data_manager chan Request) {
	// read input
	var buf = make([]byte, 1024)
	for {
		size, err := conn.Read(buf)
		fmt.Println("Got buffer")
		if err != nil {
			fmt.Println("problem reading buffer maybe exit?")
			return
		}
		fmt.Println("printing size and buffer")
		fmt.Println(size)
		fmt.Println(buf)

		// to string
		command := string(buf[:size])
		command = strings.ToLower(command)

		fmt.Println(command)

		// back to bytes
		//s := []byte(command)
		//fmt.Println(s)


//		fmt.Println(len(subs))
//		fmt.Println(subs)

		req := NewRequest(command)
//		fmt.Println(req)

		data_manager <- req
		response := <-data_manager

		conn.Write([]byte(response.Response + "\n"))

		// perform action

		// return result
	}
}

