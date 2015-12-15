# json2msgpack: convert from newline delimited json to size-header based msgpack frames

The input is a sequence of newline-delimited JSON objects.

The output is a sequence of msgpack frames. Each frame contains two msgpack encoded objects, one
inside the other. The outer msgpack object is always a variable length binary array of bytes.

Inside each binary array is the msgpack encoded object from one line of the original JSON.

This allows us to read and parse each msgpack line in turn. Otherwise parsing would be messed
up by internal newlines that could be inside the original objects. The header (outer msgpack
object) adds only 2-5 bytes per frame.

~~~
Usage of json2msgpack:
  -input string
    	path to read from (stdin default)
  -output string
    	path to write to (stdout default)
~~~
