// Copyright 2015 CloudMoDe, LLC. All rights reserved.
package mode

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/directory"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"github.com/twinj/uuid"
	"github.com/ugorji/go/codec"
	"os"
)

// Default chunk size for storage of primitives,
// 100kb limit for values in Foundation DB
const CHUNK_SIZE = 50000

var EOF = errors.New("EOF")
var NOT_FOUND = errors.New("Primitive Not Found")
var MISSING_ARG = errors.New("missing required arg")

// Primitive is analgous to the mongodb gridfs File type,
// where it specifies the details of the blob that is stored/sliced
// into the datamode.Primitive subspace. The Primitive is stored in the
// datamode.Primitive.Meta subspace
type Primitive struct {
	Id       string `json:"id"` // UUID of the primitive
	Name     string `json:"name"`
	Length   int    `json:"length"`              // number of bytes written to database
	CSize    int    `json:"chunkSize,omitempty"` // size of chunks in this primitive
	Chunks   int    `json:"chunks,omitempty"`    // total number of chunks written to database
	Created  string `json:"created,omitempty"`   // date file was created/uploaded
	Md5      string `json:"md5,omitempty"`       // md5 hash of file for comparison checking
	MimeType string `json:"mimeType,omitempty"`  // mime type
}

var pdb fdb.Database
var metaDb directory.DirectorySubspace

// set up decoder
var mph codec.Handle = new(codec.MsgpackHandle)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// init creates and opens the connection to the FDB cluster
// need something here to specifiy which cluster and also provide
// authentication
func init() {
	// init called after variable initialization when file is loaded
	// here we change uuid format to to 'Clean' which gets rid of
	// default curly braces and dashes, cutting uuid length to 32 chars
	uuid.SwitchFormat(uuid.Clean, false)

	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(300)

	// Open the default database from the system cluster
	pdb = fdb.MustOpenDefault()

	// Open the default directory 'datamode' and subjectspace for use in building keys
	mode := []string{"datamode"}
	resource := []byte("Primitive")

	directory.CreateOrOpen(pdb, mode, resource)

	// we need a number of other subspaces or directories
	// the first is for the size of the file
	// the second is for the number of chunks in the file
	// the third is for the chunk size of the file
	var metaErr error
	metaDb, metaErr = directory.CreateOrOpen(pdb, []string{"datamode", "meta"}, resource)
	checkError(metaErr)

}

// Make a new instance of Primitive, using the bytes read from the reader
// The only args required at this level of make, is the number of bytes expected
// to be on the reader, making this effectively a 'framed' read type of protocol
// header is optional, i.e. if present, will write RES_MSG onto readWriter
func (p *Primitive) Make(reader *bufio.Reader) error {
	// Create a new uuid for this primitive
	var id = uuid.NewV4().String()
	var numBytes, chunks int
	if p.Length == 0 {
		return MISSING_ARG
	}
	_, e := pdb.Transact(func(tr fdb.Transaction) (interface{}, error) {
		buf := make([]byte, CHUNK_SIZE)
		for {

			// call read on readWriter until buffer is filled, or EOF
			var readOffset int
			for {
				// get the first chunk of bytes, increment bytes read
				n1, err := reader.Read(buf[readOffset:])
				readOffset += n1
				//fmt.Printf("Primtive.Make: read:%d offset%d error:%q\n", n1, readOffset, err)
				if err == EOF || readOffset+n1 == CHUNK_SIZE {
					//fmt.Printf("Primtive.Make: filled buffer:%d byteserror:%q\n", readOffset, err)
					break
				} else if numBytes+readOffset >= p.Length {
					break
				} else if err != nil && err != EOF {
					//fmt.Printf("\nPrimitive.Make err not nil or EOF:%q", err)
					return nil, err
				}
			}

			if readOffset > 0 {
				// process bytes returned from Read before error
				numBytes = numBytes + readOffset
				// create key for this chunk
				ks := []byte(fmt.Sprintf("%s:%10d", id, chunks))
				key := fdb.Key(ks)
				chunks = chunks + 1

				//fmt.Println("Primitive.Make: check amount read:", numBytes, readOffset, p.Length)
				// check if n1 < CHUNK_SIZE, if so slice off blank end
				if readOffset < CHUNK_SIZE {
					//fmt.Println("Primitive.Make: readOffset < than chunk size:", numBytes, readOffset, chunks)
					sbuf := buf[:readOffset]
					tr.Set(key, sbuf)
					break
				} else {
					tr.Set(key, buf)
				}

			} else if readOffset == 0 {
				//fmt.Printf("\nPrimitive.Make n1 -s zero, so break")
				break
			}
		}
		//fmt.Println("Primitive.Make FINISHED READING BYTES:", numBytes, " expected:", p.Length)
		// check to see if bytes read equals number expected, stored in the original p.Size
		if p.Length != numBytes {
			//fmt.Printf("\nPrimitive.Make p.Length:%d not equal to numBytes:%d\n", p.Length, numBytes)
			return p, errors.New(fmt.Sprintf("bytes read %d doesn't match expected %d", numBytes, p.Length))
		}
		p.Id = id
		p.Chunks = chunks
		p.CSize = CHUNK_SIZE
		//fmt.Printf("\nPrimitive.Make assigning length: %d id: %s\n", numBytes, id)
		//fmt.Printf("\nPrimitive.Make length and id assigned\n")
		p.SetMeta()
		return p, nil
	})

	return e
}

// Find an instance of Primitive, using the id arg provided in the args map
// If it's found return id and number of bytes read in reply,
// otherwise return an error "Primitive Not Found"
func (p *Primitive) Find() error {
	err := p.Meta() // p is now filled out
	if err != nil {
		return err
	}
	return nil
}

func (p *Primitive) Stream(writer *bufio.Writer) error {
	var b int
	_, err := pdb.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Construct the range of all keys beginning with id. It is safe to
		// ignore the error return from PrefixRange unless the provided prefix might
		// consist entirely of zero or more 0xFF bytes.
		pr, _ := fdb.PrefixRange([]byte(p.Id))

		// Read and process the range
		iter := tr.GetRange(pr, fdb.RangeOptions{}).Iterator()

		for iter.Advance() {
			kv := iter.MustGet()
			p, e := writer.Write(kv.Value)
			if e != nil {
				// terminate, as header, and response already written
				// will be a client issue
				return 0, e
			}
			b = b + p
		}
		return b, nil
	})
	p.Length = b
	return err
}

// Destroy the bytes associated with the id arg provided in the args map
// If the file is found, return id and number of bytes destroyed in reply
// otherwise return an error "Primitive Not Found"
func (p *Primitive) Destroy() error {
	id := p.Id
	_, err := pdb.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Construct the range of all keys beginning with id. It is safe to
		// ignore the error return from PrefixRange unless the provided prefix might
		// consist entirely of zero or more 0xFF bytes.
		pr, _ := fdb.PrefixRange([]byte(id))
		// Clear the range one at a time, since we can't build
		// an Exact Range (as required by ClearRange)
		// (note: look into returning numChunks, and using that to build exact range)
		kvs := tr.GetRange(pr, fdb.RangeOptions{}).GetSliceOrPanic()
		for _, key := range kvs {
			tr.Clear(key.Key)
		}
		return nil, nil
	})
	p.DestroyMeta()
	return err
}

func (p *Primitive) Meta() error {
	if p.Id == "" || len(p.Id) != 32 {
		return errors.New(fmt.Sprintf("Invalid primtive id:%s", p.Id))
	}
	_, err := pdb.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tuple := make([]tuple.TupleElement, 1)
		tuple[0] = p.Id
		//fmt.Println("tuple:", tuple)
		packedKey := metaDb.Pack(tuple)
		value := tr.Get(packedKey).MustGet()
		//fmt.Println("value:", value)
		var dec *codec.Decoder = codec.NewDecoderBytes(value, mph)
		e := dec.Decode(p)
		return nil, e
	})
	return err
}

func (p *Primitive) SetMeta() error {
	if p.Id == "" || len(p.Id) != 32 {
		return errors.New(fmt.Sprintf("Invalid primtive id:%s", p.Id))
	}
	// 1. encode primitive to an array of bytes
	var buf []byte
	var enc *codec.Encoder = codec.NewEncoderBytes(&buf, mph) // mph is the msgpack codec
	err := enc.Encode(p)                                      // p is now encoded in buf
	if err != nil {
		//fmt.Println("error encoding primitive:", p, err)
		return err
	}
	// 2. set value of key (primitive.Id)
	_, err = pdb.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tuple := make([]tuple.TupleElement, 1)
		tuple[0] = p.Id
		//fmt.Println("tuple:", tuple)
		packedKey := metaDb.Pack(tuple)
		tr.Set(packedKey, buf)
		return nil, nil
	})
	return err
}

func (p *Primitive) DestroyMeta() error {
	if p.Id == "" || len(p.Id) != 32 {
		return errors.New(fmt.Sprintf("Invalid primtive id:%s", p.Id))
	}
	pdb.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tuple := make([]tuple.TupleElement, 1)
		tuple[0] = p.Id
		//fmt.Println("tuple:", tuple)
		packedKey := metaDb.Pack(tuple)
		tr.Clear(packedKey)
		p.Id = ""

		return nil, nil
	})
	return nil
}
