/*
json2msgpack: convert from newline delimited json to size-header based msgpack frames.

The output is a sequence of msgpack frames, each frame is a msgpack variable length binary array of bytes.
Inside each binary array is the msgpack encoded object from one line of the original JSON.
This allows us to read and parse each msgpack line in turn. Otherwise parsing would be messed
up by internal newlines that could be inside the original objects. The header adds only 2-5 bytes
per frame.
*/
package main

import (
	"bufio"
	"bytes"
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

var newline = []uint8("\n")

var ProgramName string = path.Base(os.Args[0])

type JsonMsgpConfig struct {
	InputPath  string
	OutputPath string
	Input      *os.File
	Output     *os.File
}

// call DefineFlags before myflags.Parse()
func (c *JsonMsgpConfig) DefineFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.InputPath, "input", "", "path to read from (stdin default)")
	fs.StringVar(&c.OutputPath, "output", "", "path to write to (stdout default)")
}

// call c.ValidateConfig() after myflags.Parse()
func (c *JsonMsgpConfig) ValidateConfig() error {

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

	if c.OutputPath != "" {
		if FileExists(c.OutputPath) {
			return fmt.Errorf("-output path '%s' already exists (delete/move it away it first)", c.OutputPath)
		}
		c.Output, err = os.Create(c.OutputPath)
		if err != nil {
			return err
		}
	} else {
		c.Output = os.Stdout
		c.OutputPath = "(stdout)"
	}

	return nil
}

// demonstrate the sequence of calls to DefineFlags() and ValidateConfig()
func main() {

	myflags := flag.NewFlagSet("json2msgpack", flag.ExitOnError)
	cfg := &JsonMsgpConfig{}
	cfg.DefineFlags(myflags)

	err := myflags.Parse(os.Args[1:])
	err = cfg.ValidateConfig()
	if err != nil {
		log.Fatalf("%s error: '%s'", ProgramName, err)
	}

	if cfg.InputPath != "" {
		defer cfg.Input.Close()
	}
	if cfg.OutputPath != "" {
		defer cfg.Output.Close()
	}

	status := processFile(cfg)
	os.Exit(status)
}

func processFile(cfg *JsonMsgpConfig) int {
	bufIn := bufio.NewReader(cfg.Input)
	arr := make([]byte, 0, 1024*1024)
	buf := bytes.NewBuffer(arr)

	lineNum := int64(1)
	for {
		lastLine, err := bufIn.ReadBytes('\n')
		if err != nil && err != io.EOF {
			printError(err)
			return 2
		}

		if err == io.EOF && len(lastLine) == 0 {
			break
		}

		status := jsonToMsgp(cfg, buf, lastLine, lineNum)
		if status > 0 {
			return status
		}
		buf.Reset()
		lineNum += 1

		if err == io.EOF {
			break
		}
	}

	return 0
}

// one (once upon a time newline delimited) line in js should have a complete JSON object.
func jsonToMsgp(cfg *JsonMsgpConfig, buf *bytes.Buffer, js []byte, lineNum int64) int {

	// js contains the bytes to decode from
	var jh codec.Handle = new(codec.JsonHandle)
	var dec *codec.Decoder = codec.NewDecoderBytes(js, jh)
	var iface interface{}
	var err error = dec.Decode(&iface)
	if err != nil {
		panic(fmt.Errorf("at line %d of input '%s', Decode error: '%s'", lineNum, cfg.InputPath, err))
	}

	//fmt.Printf("debug: iface = %#v\n", iface)

	var mh codec.MsgpackHandle

	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))

	// configure extensions
	// e.g. for msgpack, define functions and enable Time support for tag 1
	//mh.SetExt(reflect.TypeOf(time.Time{}), 1, myExt)

	enc := codec.NewEncoder(buf, &mh)
	err = enc.Encode(iface)
	if err != nil {
		panic(fmt.Errorf("at line %d of input '%s', Encoding error trying to encode iface='%#v': '%s'", lineNum, cfg.InputPath, iface, err))
	}

	//fmt.Printf("encoded into buf %d bytes\n", buf.Len())

	blen := buf.Len()
	if blen > 4294967295 {
		panic(fmt.Errorf("json message at line %d of '%s' is too long at %d bytes", lineNum, cfg.InputPath, blen))
	}

	// frame the output with a 2-5 byte header
	err = writeMsgpackBinArrayHeader(cfg.Output, uint32(blen))
	if err != nil {
		panic(fmt.Errorf("at line %d of input '%s', call to writeMsgpackBinArrayHeader(output='%s', blen=%d) produced error: '%s'", lineNum, cfg.InputPath, cfg.OutputPath, blen, err))
	}

	_, err = io.Copy(cfg.Output, buf)
	if err != nil {
		panic(fmt.Errorf("at line %d of input '%s', copying to output '%s' produced error: '%s'", lineNum, cfg.InputPath, cfg.OutputPath, err))
	}

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
