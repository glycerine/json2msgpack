/*
msgpackParseBench.go: measure speed of msgpack ingest by go and ugorji/go/codec
*/
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"reflect"

	"github.com/ugorji/go/codec"
)

type MsgpackHelper struct {
	initialized bool
	mh          codec.MsgpackHandle
}

func (m *MsgpackHelper) init() {
	if m.initialized {
		return
	}

	m.mh.MapType = reflect.TypeOf(map[string]interface{}(nil))

	// configure extensions
	// e.g. for msgpack, define functions and enable Time support for tag 1
	//does this make a differenece? m.mh.AddExt(reflect.TypeOf(time.Time{}), 1, timeEncExt, timeDecExt)
	m.mh.RawToString = true
	m.mh.WriteExt = true
	m.mh.SignedInteger = true
	m.mh.Canonical = true // sort maps before writing them

	m.initialized = true
}

var h MsgpackHelper

func init() {
	h.init()
}

var newline = []uint8("\n")

var ProgramName string = path.Base(os.Args[0])

type MsgpBench struct {
	InputPath string
	Input     *os.File
}

// call DefineFlags before myflags.Parse()
func (c *MsgpBench) DefineFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.InputPath, "input", "", "path to read from (stdin default)")
}

// call c.ValidateConfig() after myflags.Parse()
func (c *MsgpBench) ValidateConfig() error {

	var err error

	if c.InputPath != "" {
		if !FileExists(c.InputPath) {
			return fmt.Errorf("-input path '%s' does not exist", c.InputPath)
		}
		c.Input, err = os.OpenFile(c.InputPath, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
	} else {
		c.Input = os.Stdin
		c.InputPath = "(stdin)"
	}

	return nil
}

// demonstrate the sequence of calls to DefineFlags() and ValidateConfig()
func main() {

	myflags := flag.NewFlagSet("msgpackParseBench", flag.ExitOnError)
	cfg := &MsgpBench{}
	cfg.DefineFlags(myflags)

	err := myflags.Parse(os.Args[1:])
	err = cfg.ValidateConfig()
	if err != nil {
		log.Fatalf("%s error: '%s'", ProgramName, err)
	}

	if cfg.InputPath != "" {
		defer cfg.Input.Close()
	}

	status := processFile(cfg)
	os.Exit(status)
}

func processFile(cfg *MsgpBench) int {

	h.init()
	var r interface{}
	var s interface{}

	bufIn := bufio.NewReader(cfg.Input)
	decoder := codec.NewDecoder(bufIn, &h.mh)

	k := 0
	var err error
	for {
		err = decoder.Decode(&r)
		if err != nil {
			break
		}
		switch val := r.(type) {
		case []byte:
			//fmt.Printf("got a []byte\n")
			headerSz, _, _, err := DecodeMsgpackBinArrayHeader(val[:])
			if err != nil {
				break
			}

			decodeSlice := codec.NewDecoderBytes(val[headerSz:], &h.mh)
			err = decodeSlice.Decode(&s)
			if err != nil {
				break
			}
			//fmt.Printf("decoded a []byte into: '%#v'\n", s)
			k++
		default:
			panic(fmt.Sprintf("unexpected %T with val = '%#v'", val, val))
		}
	}

	fmt.Printf("parsed a total of %d msgpack frames.\n", k)
	return 0
}

func printError(err error) {
	os.Stdout.Sync()
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

func writeMsgpackBinArrayHeader(w io.Writer, l uint32) error {
	var by [8]byte
	var nBytesAdded int
	if l < 256 {
		by[0] = 0xc4 // msgpackBin8
		by[1] = uint8(l)
		nBytesAdded = 2
	} else if l < 65536 {
		by[0] = 0xc5 // msgpackBin16
		binary.BigEndian.PutUint16(by[1:3], uint16(l))
		nBytesAdded = 3
	} else {
		by[0] = 0xc6 // msgpackBin32
		binary.BigEndian.PutUint32(by[1:5], l)
		nBytesAdded = 5
	}
	_, err := w.Write(by[:nBytesAdded])
	return err
}

func FileExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

func DirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func DecodeMsgpackBinArrayHeader(p []byte) (headerSize int, payloadSize int, totalFrameSize int, err error) {
	lenp := len(p)

	switch p[0] {
	case 0xc4: // msgpackBin8
		if lenp < 2 {
			err = fmt.Errorf("DecodeMsgpackBinArrayHeader error: p len (%d) too small", lenp)
			return
		}
		headerSize = 2
		payloadSize = int(p[1])
	case 0xc5: // msgpackBin16
		if lenp < 3 {
			err = fmt.Errorf("DecodeMsgpackBinArrayHeader error: p len (%d) too small", lenp)
			return
		}
		headerSize = 3
		payloadSize = int(binary.BigEndian.Uint16(p[1:3]))
	case 0xc6: // msgpackBin32
		if lenp < 5 {
			err = fmt.Errorf("DecodeMsgpackBinArrayHeader error: p len (%d) too small", lenp)
			return
		}
		headerSize = 5
		payloadSize = int(binary.BigEndian.Uint32(p[1:5]))
	}

	totalFrameSize = headerSize + payloadSize
	return
}
