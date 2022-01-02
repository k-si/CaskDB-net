package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/peterh/liner"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var commands = map[string]int{
	// string
	"set":    0,
	"mset":   1,
	"setnx":  2,
	"msetnx": 3,
	"get":    4,
	"mget":   5,
	"getset": 6,
	"remove": 7,
	"slen":   8,
	// hash
	"hset":    9,
	"hsetnx":  10,
	"hget":    11,
	"hgetall": 12,
	"hdel":    13,
	"hlen":    14,
	"hexist":  15,
	// list
	"lpush":    16,
	"lrpush":   17,
	"lpop":     18,
	"lrPop":    19,
	"linsert":  20,
	"lrinsert": 21,
	"lset":     22,
	"lrem":     23,
	"llen":     24,
	"lindex":   25,
	"lrange":   26,
	"lexist":   27,
	// set
	"sadd":      28,
	"srem":      29,
	"smove":     30,
	"sunion":    31,
	"sdiff":     32,
	"sscan":     33,
	"scard":     34,
	"sismember": 35,
	// zset
	"zadd":        36,
	"zrem":        37,
	"zscorerange": 38,
	"zscore":      39,
	"zcard":       40,
	"zismember":   41,
	"ztop":        42,
}

const HistoryPath = "/tmp/CaskDB-cli"

type Message struct {
	id     uint32
	length uint32
	data   []byte
}

func main() {
	h := flag.String("host", "127.0.0.1", "tcp server address")
	p := flag.Int("port", 4519, "tcp server port")
	flag.Parse()
	if *h == "" {
		*h = "127.0.0.1"
	}
	if *p == 0 {
		*p = 4519
	}

	// connect
	addr := fmt.Sprintf("%s:%d", *h, *p)
	conn, err := net.Dial("tcp4", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// client heart beat
	go heartBeat(conn)

	// new liner
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	// load command history
	if f, err := os.Open(HistoryPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	defer func() {
		if f, err := os.Create(HistoryPath); err != nil {
			fmt.Printf("writing cmd history err: %v\n", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	prompt := addr + ">"
	for {
		cmd, err := line.Prompt(prompt)
		if err != nil {
			fmt.Println(err)
			break
		}
		// check
		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}
		line.AppendHistory(cmd)
		command := parseCommand(cmd)
		if command[0] == "quit" {
			break
		} else {
			if !checkCommand(command) {
				fmt.Println("bad parameter")
				continue
			}
			// do request
			if err := handle(conn, command); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func handle(conn net.Conn, c []string) error {

	// prepare data
	id := uint32(commands[c[0]])
	var data bytes.Buffer
	for i := 1; i < len(c); i++ {
		data.Write([]byte(c[i]))
		if i < len(c)-1 {
			data.Write([]byte(" "))
		}
	}
	binMsg, err := Pack(id, data.Bytes())
	if err != nil {
		return err
	}

	// send
	if _, err = conn.Write(binMsg); err != nil {
		return err
	}

	// read head
	headBuf := make([]byte, 8)
	if _, err = io.ReadFull(conn, headBuf); err != nil {
		return err
	}
	msg, err := UnPack(headBuf)
	if err != nil {
		return err
	}

	// read data
	length := msg.length
	dataBuf := make([]byte, length)
	if _, err = io.ReadFull(conn, dataBuf); err != nil {
		return err
	}
	fmt.Println(string(dataBuf))

	return nil
}

func heartBeat(conn net.Conn) {
	for {
		time.Sleep(30 * time.Second)
		binMsg, err := Pack(100, []byte(""))
		if err != nil {
			fmt.Println("data pack err:", err)
			continue
		}
		conn.Write(binMsg)
	}
}

// 将message转为二进制切片
func Pack(id uint32, data []byte) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})

	if err := binary.Write(buf, binary.LittleEndian, id); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(data))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// 将二进制数据中的head抽离出来
func UnPack(data []byte) (*Message, error) {
	buf := bytes.NewBuffer(data)
	head := &Message{}

	if err := binary.Read(buf, binary.LittleEndian, &head.id); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &head.length); err != nil {
		return nil, err
	}

	return head, nil
}

func parseCommand(cmdLine string) []string {
	arr := strings.Split(cmdLine, " ")
	if len(arr) == 0 {
		return nil
	}
	args := make([]string, 0)
	for i := 0; i < len(arr); i++ {
		if arr[i] == "" {
			continue
		}
		if i == 0 {
			args = append(args, strings.ToLower(arr[i]))
		} else {
			args = append(args, arr[i])
		}
	}
	return args
}

func checkCommand(command []string) bool {
	if _, ok := commands[command[0]]; !ok {
		return false
	}
	switch command[0] {
	case "set":
		if len(command) != 3 {
			return false
		}
	case "mset":
		if len(command) < 3 {
			return false
		}
	case "setnx":
		if len(command) != 3 {
			return false
		}
	case "get":
		if len(command) != 2 {
			return false
		}
	case "mget":
		if len(command) < 2 {
			return false
		}
	case "getset":
		if len(command) != 3 {
			return false
		}
	case "remove":
		if len(command) != 2 {
			return false
		}
	case "slen":
		if len(command) != 1 {
			return false
		}
	case "hset":
		if len(command) != 4 {
			return false
		}
	case "hsetnx":
		if len(command) != 4 {
			return false
		}
	case "hget":
		if len(command) != 3 {
			return false
		}
	case "hgetall":
		if len(command) != 2 {
			return false
		}
	case "hdel":
		if len(command) != 3 {
			return false
		}
	case "hlen":
		if len(command) != 2 {
			return false
		}
	case "hexist":
		if len(command) != 3 {
			return false
		}
	case "lpush":
		if len(command) < 3 {
			return false
		}
	case "lrpush":
		if len(command) < 3 {
			return false
		}
	case "lpop":
		if len(command) != 2 {
			return false
		}
	case "lrpop":
		if len(command) != 2 {
			return false
		}
	case "linsert":
		if len(command) != 4 {
			return false
		}
	case "lrinsert":
		if len(command) != 4 {
			return false
		}
	case "lset":
		if len(command) != 4 {
			return false
		}
	case "lrem":
		if len(command) != 4 {
			return false
		}
	case "llen":
		if len(command) != 2 {
			return false
		}
	case "lindex":
		if len(command) != 3 {
			return false
		}
	case "lrange":
		if len(command) != 4 {
			return false
		}
	case "lexist":
		if len(command) != 3 {
			return false
		}
	case "sadd":
		if len(command) < 3 {
			return false
		}
	case "srem":
		if len(command) != 3 {
			return false
		}
	case "smove":
		if len(command) != 4 {
			return false
		}
	case "sunion":
		if len(command) < 2 {
			return false
		}
	case "sdiff":
		if len(command) < 2 {
			return false
		}
	case "sscan":
		if len(command) != 2 {
			return false
		}
	case "scard":
		if len(command) != 2 {
			return false
		}
	case "sismember":
		if len(command) != 3 {
			return false
		}
	case "zadd":
		if len(command) != 4 {
			return false
		}
	case "zrem":
		if len(command) != 3 {
			return false
		}
	case "zscorerange":
		if len(command) != 4 {
			return false
		}
	case "zscore":
		if len(command) != 3 {
			return false
		}
	case "zcard":
		if len(command) != 2 {
			return false
		}
	case "zismember":
		if len(command) != 3 {
			return false
		}
	}
	return true
}
