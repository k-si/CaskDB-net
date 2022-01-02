package main

import (
	"flag"
	"fmt"
	"github.com/k-si/CaskDB"
	"github.com/k-si/Kinx/kiface"
	"github.com/k-si/Kinx/knet"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"
)

const (
	// net
	DefaultIPVersion         = "tcp4"
	DefaultHost              = "127.0.0.1"
	DefaultPort              = 4519
	DefaultMaxConnSize       = 1
	DefaultMaxPackageSize    = 1024 * 1024 // 1mb
	DefaultWorkerPoolSize    = 1
	DefaultMaxWorkerTaskSize = 100
	DefaultHeartRateInSecond = 30 * time.Second
	DefaultHeartFreshLevel   = 5
	DefaultHeartPackageId    = 100

	// db
	DefaultDBDir         = "/tmp/CaskDB"
	DefaultMaxKeySize    = 1 * 1024        // 1kb
	DefaultMaxValueSize  = 8 * 1024        // 8kb
	DefaultMaxFileSize   = 1 * 1024 * 1024 // 1mb
	DefaultMergeInterval = 24 * time.Hour
	DefaultWriteSync     = false
)

type Server struct {
	netServer kiface.IServer
	dbServer  *CaskDB.DB
}

type ServerConfig struct {
	// net
	IPVersion         string        `json:"ip_version" yaml:"ip_version" toml:"ip_version"`
	Host              string        `json:"host" yaml:"host" toml:"host"`
	Port              int           `json:"port" yaml:"port" toml:"port"`
	MaxConnSize       int           `json:"max_conn_size" yaml:"max_conn_size" toml:"max_conn_size"`
	MaxPackageSize    uint32        `json:"max_package_size" yaml:"max_package_size" toml:"max_package_size"`
	WorkerPoolSize    uint32        `json:"work_pool_size" yaml:"work_pool_size" toml:"work_pool_size"`
	MaxWorkerTaskSize uint32        `json:"max_worker_task" yaml:"max_worker_task" toml:"max_worker_task"`
	HeartRateInSecond time.Duration `json:"heart_rate_in_sec" yaml:"heart_rate_in_sec" toml:"heart_rate_in_sec"`
	HeartFreshLevel   uint32        `json:"heart_fresh_level" yaml:"heart_fresh_level" toml:"heart_fresh_level"`
	HeartPackageId    uint32        `json:"heart_package_id" yaml:"heart_package_id" toml:"heart_package_id"`

	// db
	DBDir         string        `json:"db_dir" yaml:"db_dir" toml:"db_dir"`
	MaxKeySize    uint32        `json:"max_key_size" yaml:"max_key_size" toml:"max_key_size"`
	MaxValueSize  uint32        `json:"max_val_size" yaml:"max_val_size" toml:"max_val_size"`
	MaxFileSize   int64         `json:"max_file_size" yaml:"max_file_size" toml:"max_file_size"`
	MergeInterval time.Duration `json:"gc_interval" yaml:"gc_interval" toml:"gc_interval"`
	WriteSync     bool          `json:"sync_now" yaml:"sync_now" toml:"sync_now"`
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		// net
		IPVersion:         DefaultIPVersion,
		Host:              DefaultHost,
		Port:              DefaultPort,
		MaxConnSize:       DefaultMaxConnSize,
		MaxPackageSize:    DefaultMaxPackageSize,
		WorkerPoolSize:    DefaultWorkerPoolSize,
		MaxWorkerTaskSize: DefaultMaxWorkerTaskSize,
		HeartRateInSecond: DefaultHeartRateInSecond,
		HeartFreshLevel:   DefaultHeartFreshLevel,
		HeartPackageId:    DefaultHeartPackageId,
		// db
		DBDir:        DefaultDBDir,
		MaxKeySize:   DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
		MaxFileSize:  DefaultMaxFileSize,
		//MergeInterval: DefaultMergeInterval,
		WriteSync: DefaultWriteSync,
	}
}

// string
type SetRouter struct {
	knet.BaseRouter
}

func (sr *SetRouter) Handle(req kiface.IRequest) {
	log.Println("handle Set")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.Set(c[0], c[1])
	if err != nil {
		if err := req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type MSetRouter struct {
	knet.BaseRouter
}

func (msr *MSetRouter) Handle(req kiface.IRequest) {
	log.Println("handle MSet")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.MSet(c...)
	if err != nil {
		if err := req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type SetNxRouter struct {
	knet.BaseRouter
}

func (snr *SetNxRouter) Handle(req kiface.IRequest) {
	log.Println("handle SetNx")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.SetNx(c[0], c[1])
	if err != nil {
		if err := req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type MSetNxRouter struct {
	knet.BaseRouter
}

func (msnr *MSetNxRouter) Handle(req kiface.IRequest) {
	log.Println("handle MSetNx")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.MSetNx(c...)
	if err != nil {
		if err := req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type GetRouter struct {
	knet.BaseRouter
}

func (gr *GetRouter) Handle(req kiface.IRequest) {
	log.Println("handle Get")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.Get(c[0])
	if err != nil {
		if err := req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if res == nil || len(res) == 0 {
			if err := req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
				log.Println(err)
			}
		} else {
			if err := req.GetConnection().SendMessage(200, res); err != nil {
				log.Println(err)
			}
		}
	}
}

type MGetRouter struct {
	knet.BaseRouter
}

func (mgr *MGetRouter) Handle(req kiface.IRequest) {
	log.Println("handle MGet")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.MGet(c...)
	if err != nil {
		if err = req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		b := strings.Builder{}
		for i, r := range res {
			if r == nil || len(r) == 0 {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") (nil)")
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			} else {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") ")
				b.WriteString(string(r))
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type GetSetRouter struct {
	knet.BaseRouter
}

func (gsr *GetSetRouter) Handle(req kiface.IRequest) {
	log.Println("handle GetSet")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.GetSet(c[0], c[1])
	if err != nil {
		if err = req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
				log.Println(err)
			}
		} else {
			if err = req.GetConnection().SendMessage(200, res); err != nil {
				log.Println(err)
			}
		}
	}
}

type RemoveRouter struct {
	knet.BaseRouter
}

func (rr *RemoveRouter) Handle(req kiface.IRequest) {
	log.Println("handle Remove")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.Remove(c[0])
	if err != nil {
		if err = req.GetConnection().SendMessage(400, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type SLenRouter struct {
	knet.BaseRouter
}

func (slr *SLenRouter) Handle(req kiface.IRequest) {
	log.Println("handle SLen")

	l := s.dbServer.StrLen()
	if err := req.GetConnection().SendMessage(200, []byte(strconv.Itoa(l))); err != nil {
		log.Println(err)
	}
}

// hash
type HSetRouter struct {
	knet.BaseRouter
}

func (hsr *HSetRouter) Handle(req kiface.IRequest) {
	log.Println("handle HSet")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.HSet(c[0], c[1], c[2])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type HSetNxRouter struct {
	knet.BaseRouter
}

func (hsnr *HSetNxRouter) Handle(req kiface.IRequest) {
	log.Println("handle HSetNx")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.HSetNx(c[0], c[1], c[2])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type HGetRouter struct {
	knet.BaseRouter
}

func (hg *HGetRouter) Handle(req kiface.IRequest) {
	log.Println("handle HGet")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.HGet(c[0], c[1])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if res == nil || len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
				log.Println(err)
			}
		}
		if err = req.GetConnection().SendMessage(200, res); err != nil {
			log.Println(err)
		}
	}
}

type HGetAllRouter struct {
	knet.BaseRouter
}

func (hgar *HGetAllRouter) Handle(req kiface.IRequest) {
	log.Println("handle HGetAll")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.HGetAll(c[0])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i, r := range res {
			if len(r) == 0 {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") (nil)")
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			} else {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") ")
				b.WriteString(string(r))
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type HDelRouter struct {
	knet.BaseRouter
}

func (hdr *HDelRouter) Handle(req kiface.IRequest) {
	log.Println("handle HDel")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.HDel(c[0], c[1])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type HLenRouter struct {
	knet.BaseRouter
}

func (hlr *HLenRouter) Handle(req kiface.IRequest) {
	log.Println("handle HLen")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	l := s.dbServer.HLen(c[0])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.Itoa(l))); err != nil {
		log.Println(err)
	}
}

type HExistRouter struct {
	knet.BaseRouter
}

func (her *HExistRouter) Handle(req kiface.IRequest) {
	log.Println("handle HExist")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	b := s.dbServer.HExist(c[0], c[1])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.FormatBool(b))); err != nil {
		log.Println(err)
	}
}

// list
type LPushRouter struct {
	knet.BaseRouter
}

func (lpr *LPushRouter) Handle(req kiface.IRequest) {
	log.Println("handle LPush")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.LPush(c[0], c[1:]...)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type LRPushRouter struct {
	knet.BaseRouter
}

func (lrpr *LRPushRouter) Handle(req kiface.IRequest) {
	log.Println("handle LRPush")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.RPush(c[0], c[1:]...)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type LPopRouter struct {
	knet.BaseRouter
}

func (lpr *LPopRouter) Handle(req kiface.IRequest) {
	log.Println("handle LPop")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.LPop(c[0])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if res == nil || len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
				log.Println(err)
			}
		} else {
			if err = req.GetConnection().SendMessage(200, res); err != nil {
				log.Println(err)
			}
		}
	}
}

type LRPopRouter struct {
	knet.BaseRouter
}

func (lrpr *LRPopRouter) Handle(req kiface.IRequest) {
	log.Println("handle RPop")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.RPop(c[0])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if res == nil || len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
				log.Println(err)
			}
		} else {
			if err = req.GetConnection().SendMessage(200, res); err != nil {
				log.Println(err)
			}
		}
	}
}

type LInsertRouter struct {
	knet.BaseRouter
}

func (lir *LInsertRouter) Handle(req kiface.IRequest) {
	log.Println("handle LInsert")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n, _ := strconv.Atoi(string(c[2]))

	err := s.dbServer.LInsert(c[0], c[1], n)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type LRInsertRouter struct {
	knet.BaseRouter
}

func (lrir *LRInsertRouter) Handle(req kiface.IRequest) {
	log.Println("handle LRInsert")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n, _ := strconv.Atoi(string(c[2]))

	err := s.dbServer.RInsert(c[0], c[1], n)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type LSetRouter struct {
	knet.BaseRouter
}

func (lsr *LSetRouter) Handle(req kiface.IRequest) {
	log.Println("handle LSet")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n, _ := strconv.Atoi(string(c[2]))

	err := s.dbServer.LSet(c[0], c[1], n)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type LRemRouter struct {
	knet.BaseRouter
}

func (lrr *LRemRouter) Handle(req kiface.IRequest) {
	log.Println("handle LRem")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n, _ := strconv.Atoi(string(c[2]))

	err := s.dbServer.LRem(c[0], c[1], n)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type LLenRouter struct {
	knet.BaseRouter
}

func (llr *LLenRouter) Handle(req kiface.IRequest) {
	log.Println("handle LLen")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	l := s.dbServer.LLen(c[0])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.Itoa(l))); err != nil {
		log.Println(err)
	}
}

type LIndexRouter struct {
	knet.BaseRouter
}

func (lir *LIndexRouter) Handle(req kiface.IRequest) {
	log.Println("handle LIndex")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n, _ := strconv.Atoi(string(c[1]))

	res, err := s.dbServer.LIndex(c[0], n)

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if res == nil || len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
				log.Println(err)
			}
		} else {
			if err = req.GetConnection().SendMessage(200, res); err != nil {
				log.Println(err)
			}
		}
	}
}

type LRangeRouter struct {
	knet.BaseRouter
}

func (lrr *LRangeRouter) Handle(req kiface.IRequest) {
	log.Println("handle LRange")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	start, _ := strconv.Atoi(string(c[1]))
	stop, _ := strconv.Atoi(string(c[2]))

	res, err := s.dbServer.LRange(c[0], start, stop)

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i, r := range res {
			if len(r) == 0 {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") (nil)")
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			} else {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") ")
				b.WriteString(string(r))
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type LExistRouter struct {
	knet.BaseRouter
}

func (ler *LExistRouter) Handle(req kiface.IRequest) {
	log.Println("handle LExist")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	b := s.dbServer.LExist(c[0], c[1])

	if err := req.GetConnection().SendMessage(200, []byte(strconv.FormatBool(b))); err != nil {
		log.Println(err)
	}
}

// set
type SAddRouter struct {
	knet.BaseRouter
}

func (sar *SAddRouter) Handle(req kiface.IRequest) {
	log.Println("handle SAdd")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.SAdd(c[0], c[1:]...)
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type SRemRouter struct {
	knet.BaseRouter
}

func (srr *SRemRouter) Handle(req kiface.IRequest) {
	log.Println("handle SRem")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.SRem(c[0], c[1])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type SMoveRouter struct {
	knet.BaseRouter
}

func (smr *SMoveRouter) Handle(req kiface.IRequest) {
	log.Println("handle SMove")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.SMove(c[0], c[1], c[2])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type SUnionRouter struct {
	knet.BaseRouter
}

func (sur *SUnionRouter) Handle(req kiface.IRequest) {
	log.Println("handle SUnion")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.SUnion(c...)

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i, r := range res {
			if len(r) == 0 {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") (nil)")
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			} else {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") ")
				b.WriteString(string(r))
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type SDiffRouter struct {
	knet.BaseRouter
}

func (sdr *SDiffRouter) Handle(req kiface.IRequest) {
	log.Println("handle SDiff")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.SDiff(c...)

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i, r := range res {
			if len(r) == 0 {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") (nil)")
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			} else {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") ")
				b.WriteString(string(r))
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type SScanRouter struct {
	knet.BaseRouter
}

func (ssr *SScanRouter) Handle(req kiface.IRequest) {
	log.Println("handle SScan")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	res, err := s.dbServer.SScan(c[0])

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i, r := range res {
			if len(r) == 0 {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") (nil)")
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			} else {
				b.WriteString(strconv.Itoa(i))
				b.WriteString(") ")
				b.WriteString(string(r))
				if i < len(res)-1 {
					b.WriteString("\n")
				}
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type SCardRouter struct {
	knet.BaseRouter
}

func (scr *SCardRouter) Handle(req kiface.IRequest) {
	log.Println("handle SCard")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	l := s.dbServer.SCard(c[0])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.Itoa(l))); err != nil {
		log.Println(err)
	}
}

type SIsMemberRouter struct {
	knet.BaseRouter
}

func (simr *SIsMemberRouter) Handle(req kiface.IRequest) {
	log.Println("handle SIsMember")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	b := s.dbServer.SIsMember(c[0], c[1])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.FormatBool(b))); err != nil {
		log.Println(err)
	}
}

// zset
type ZAddRouter struct {
	knet.BaseRouter
}

func (zar *ZAddRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZAdd")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	score, _ := strconv.ParseFloat(string(c[1]), 64)
	log.Println(score)

	err := s.dbServer.ZAdd(c[0], score, c[2])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type ZRemRouter struct {
	knet.BaseRouter
}

func (zrr *ZRemRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZRem")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	err := s.dbServer.ZRem(c[0], c[1])
	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if err = req.GetConnection().SendMessage(200, []byte("\"OK\"")); err != nil {
			log.Println(err)
		}
	}
}

type ZScoreRangeRouter struct {
	knet.BaseRouter
}

func (zsrr *ZScoreRangeRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZScoreRange")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	from, _ := strconv.ParseFloat(string(c[1]), 64)
	to, _ := strconv.ParseFloat(string(c[2]), 64)

	res, err := s.dbServer.ZScoreRange(c[0], from, to)

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i := 0; i < len(res); i += 2 {
			// write member
			b.WriteString(strconv.Itoa(i))
			b.WriteString(") ")
			b.WriteString(res[i].(string))
			if i < len(res)-1 {
				b.WriteString("\n")
			}
			// write score
			b.WriteString(strconv.Itoa(i + 1))
			b.WriteString(") ")
			b.WriteString(fmt.Sprintf("%f", res[i+1].(float64)))
			if i < len(res)-2 {
				b.WriteString("\n")
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

type ZScoreRouter struct {
	knet.BaseRouter
}

func (zsr *ZScoreRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZScore")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	b, res := s.dbServer.ZScore(c[0], c[1])
	score := fmt.Sprintf("%f", res)
	if b {
		if err := req.GetConnection().SendMessage(200, []byte(score)); err != nil {
			log.Println(err)
		}
	} else {
		if err := req.GetConnection().SendMessage(200, []byte("(nil)")); err != nil {
			log.Println(err)
		}
	}
}

type ZCardRouter struct {
	knet.BaseRouter
}

func (zcr *ZCardRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZCard")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n := s.dbServer.ZCard(c[0])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.Itoa(n))); err != nil {
		log.Println(err)
	}
}

type ZIsMemberRouter struct {
	knet.BaseRouter
}

func (zimr *ZIsMemberRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZIsMember")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	b := s.dbServer.ZIsMember(c[0], c[1])
	if err := req.GetConnection().SendMessage(200, []byte(strconv.FormatBool(b))); err != nil {
		log.Println(err)
	}
}

type ZTopRouter struct {
	knet.BaseRouter
}

func (ztr *ZTopRouter) Handle(req kiface.IRequest) {
	log.Println("handle ZTop")
	c := parseCommand(string(req.GetMsg().GetMsgData()))

	n, _ := strconv.Atoi(string(c[1]))
	res, err := s.dbServer.ZTop(c[0], n)

	if err != nil {
		if err = req.GetConnection().SendMessage(200, []byte(err.Error())); err != nil {
			log.Println(err)
		}
	} else {
		if len(res) == 0 {
			if err = req.GetConnection().SendMessage(200, []byte("(empty list)")); err != nil {
				log.Println(err)
			}
			return
		}
		b := strings.Builder{}
		for i := 0; i < len(res); i += 2 {
			// write member
			b.WriteString(strconv.Itoa(i))
			b.WriteString(") ")
			b.WriteString(res[i].(string))
			if i < len(res)-1 {
				b.WriteString("\n")
			}
			// write score
			b.WriteString(strconv.Itoa(i + 1))
			b.WriteString(") ")
			b.WriteString(fmt.Sprintf("%f", res[i+1].(float64)))
			if i < len(res)-2 {
				b.WriteString("\n")
			}
		}
		if err = req.GetConnection().SendMessage(200, []byte(b.String())); err != nil {
			log.Println(err)
		}
	}
}

var s *Server

func init() {
	b, _ := ioutil.ReadFile("./banner.txt")
	fmt.Println(string(b))
}

func main() {

	// pprof
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("server panic: %+v", r)
		}
	}()

	// get command flag
	c := flag.String("c", "", "Profile path")
	flag.Parse()

	// load configuration
	var cfg ServerConfig
	if *c == "" {
		cfg = DefaultServerConfig()
	} else {
		tmp, err := loadConfig(*c)
		if err != nil {
			log.Fatal(err)
		}
		cfg = *tmp
	}

	ss, err := NewServer(cfg)
	s = ss
	if err != nil {
		log.Fatal(err)
	}

	// registry router
	ns := s.netServer
	ns.AddRouter(0, &SetRouter{})
	ns.AddRouter(1, &MSetRouter{})
	ns.AddRouter(2, &SetNxRouter{})
	ns.AddRouter(3, &MSetNxRouter{})
	ns.AddRouter(4, &GetRouter{})
	ns.AddRouter(5, &MGetRouter{})
	ns.AddRouter(6, &GetSetRouter{})
	ns.AddRouter(7, &RemoveRouter{})
	ns.AddRouter(8, &SLenRouter{})
	ns.AddRouter(9, &HSetRouter{})
	ns.AddRouter(10, &HSetNxRouter{})
	ns.AddRouter(11, &HGetRouter{})
	ns.AddRouter(12, &HGetAllRouter{})
	ns.AddRouter(13, &HDelRouter{})
	ns.AddRouter(14, &HLenRouter{})
	ns.AddRouter(15, &HExistRouter{})
	ns.AddRouter(16, &LPushRouter{})
	ns.AddRouter(17, &LRPushRouter{})
	ns.AddRouter(18, &LPopRouter{})
	ns.AddRouter(19, &LRPopRouter{})
	ns.AddRouter(20, &LInsertRouter{})
	ns.AddRouter(21, &LRInsertRouter{})
	ns.AddRouter(22, &LSetRouter{})
	ns.AddRouter(23, &LRemRouter{})
	ns.AddRouter(24, &LLenRouter{})
	ns.AddRouter(25, &LIndexRouter{})
	ns.AddRouter(26, &LRangeRouter{})
	ns.AddRouter(27, &LExistRouter{})
	ns.AddRouter(28, &SAddRouter{})
	ns.AddRouter(29, &SRemRouter{})
	ns.AddRouter(30, &SMoveRouter{})
	ns.AddRouter(31, &SUnionRouter{})
	ns.AddRouter(32, &SDiffRouter{})
	ns.AddRouter(33, &SScanRouter{})
	ns.AddRouter(34, &SCardRouter{})
	ns.AddRouter(35, &SIsMemberRouter{})
	ns.AddRouter(36, &ZAddRouter{})
	ns.AddRouter(37, &ZRemRouter{})
	ns.AddRouter(38, &ZScoreRangeRouter{})
	ns.AddRouter(39, &ZScoreRouter{})
	ns.AddRouter(40, &ZCardRouter{})
	ns.AddRouter(41, &ZIsMemberRouter{})
	ns.AddRouter(42, &ZTopRouter{})

	// tcp server blocking
	ns.Serve()
	defer s.dbServer.Close()
}

func NewServer(cfg ServerConfig) (*Server, error) {

	// load tcp server config
	netCfg := knet.DefaultConfig()
	netCfg.IPVersion = cfg.IPVersion
	netCfg.Host = cfg.Host
	netCfg.TcpPort = cfg.Port
	netCfg.WorkerPoolSize = cfg.WorkerPoolSize
	netCfg.MaxConnSize = cfg.MaxConnSize
	netCfg.MaxPackageSize = cfg.MaxPackageSize
	netCfg.MaxWorkerTaskSize = cfg.MaxWorkerTaskSize
	//netCfg.HeartRateInSecond = cfg.HeartRateInSecond
	netCfg.HeartFreshLevel = cfg.HeartFreshLevel

	netServer := knet.NewServer(netCfg)

	// load db server config
	dbCfg := CaskDB.DefaultConfig()
	dbCfg.DBDir = cfg.DBDir
	dbCfg.MaxKeySize = cfg.MaxKeySize
	dbCfg.MaxValueSize = cfg.MaxKeySize
	dbCfg.MaxFileSize = cfg.MaxFileSize
	//dbCfg.MergeInterval = cfg.MergeInterval
	dbCfg.WriteSync = cfg.WriteSync
	dbServer, err := CaskDB.Open(dbCfg)
	if err != nil {
		return nil, err
	}

	s := &Server{
		netServer: netServer,
		dbServer:  dbServer,
	}
	return s, nil
}

func loadConfig(path string) (*ServerConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &ServerConfig{}
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseCommand(cmdLine string) [][]byte {
	arr := strings.Split(cmdLine, " ")
	var args [][]byte

	for i := 0; i < len(arr); i++ {
		args = append(args, []byte(arr[i]))
	}
	return args
}
