/*
jsonParseBench.go: measure speed of json ingest by go and ugorji/go/codec
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

	"github.com/ugorji/go/codec"
)

var ProgramName string = path.Base(os.Args[0])

type JsonBench struct {
	InputPath string
	Input     *os.File
}

// call DefineFlags before myflags.Parse()
func (c *JsonBench) DefineFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.InputPath, "input", "", "path to read from (stdin default)")
}

// call c.ValidateConfig() after myflags.Parse()
func (c *JsonBench) ValidateConfig() error {

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

	myflags := flag.NewFlagSet("jsonParseBench", flag.ExitOnError)
	cfg := &JsonBench{}
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

func processFile(cfg *JsonBench) int {
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

		status := jsonDecode(cfg, buf, lastLine, lineNum)
		if status > 0 {
			return status
		}
		buf.Reset()
		if err == io.EOF {
			break
		}
		lineNum += 1
	}

	fmt.Printf("parsed %d lines of json\n", lineNum-1)
	return 0
}

// one (once upon a time newline delimited) line in js should have a complete JSON object.
func jsonDecode(cfg *JsonBench, buf *bytes.Buffer, js []byte, lineNum int64) int {

	// js contains the bytes to decode from
	var jh codec.Handle = new(codec.JsonHandle)
	var dec *codec.Decoder = codec.NewDecoderBytes(js, jh)
	var iface interface{}
	var err error = dec.Decode(&iface)
	if err != nil {
		panic(fmt.Errorf("at line %d of input '%s', Decode error: '%s'", lineNum, cfg.InputPath, err))
	}

	//fmt.Printf("debug: iface = %#v\n", iface)
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
