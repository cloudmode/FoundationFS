// Copyright 2015 CloudMoDe, LLC. All rights reserved.
package mode

import (
	"bufio"
	//"fmt"
	"github.com/FoundationFS/mode"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestPrimitive(t *testing.T) {
	Convey("Set the meta data for a non-existent Primitive", t, func() {
		primitive := mode.Primitive{"e64a919ef57c4481bcd5fba43f8efb9c", "sample.jpg", 4, 5, 6, "one", "two", "image/jpg"}
		err := primitive.SetMeta()
		So(err, ShouldEqual, nil)
		//fmt.Println("Primitive.Meta:", primitive)
		So(primitive.MimeType, ShouldEqual, "image/jpg")
	})
	Convey("Read the meta data for the same Primitive", t, func() {
		primitive := mode.Primitive{"e64a919ef57c4481bcd5fba43f8efb9c", "", 0, 0, 0, "", "", ""}
		err := primitive.Meta()
		So(err, ShouldEqual, nil)
		//fmt.Println("Primitive.Meta:", primitive)
		So(primitive.MimeType, ShouldEqual, "image/jpg")
		So(primitive.Name, ShouldEqual, "sample.jpg")
	})
	Convey("Read the meta data for the same Primitive", t, func() {
		primitive := mode.Primitive{"e64a919ef57c4481bcd5fba43f8efb9c", "", 0, 0, 0, "", "", ""}
		err := primitive.DestroyMeta()
		So(err, ShouldEqual, nil)
		//fmt.Println("Primitive.Meta:", primitive)
		So(primitive.Id, ShouldEqual, "")
		So(primitive.Name, ShouldEqual, "")
		So(primitive.MimeType, ShouldEqual, "")
	})
	Convey("Meta data should not exist", t, func() {
		primitive := mode.Primitive{"e64a919ef57c4481bcd5fba43f8efb9c", "", 0, 0, 0, "", "", ""}
		err := primitive.Meta()
		So(err, ShouldNotEqual, nil)
		//fmt.Println("Primitive.Meta:", primitive)
		So(primitive.Id, ShouldEqual, "e64a919ef57c4481bcd5fba43f8efb9c")
		So(primitive.Name, ShouldEqual, "")
	})

	dataDir := "./data/"
	files, errDir := ioutil.ReadDir(dataDir)

	Convey("There should be at least one file in test data directory", t, func() {
		So(errDir, ShouldEqual, nil)
		So(len(files), ShouldBeGreaterThanOrEqualTo, 1)
	})

	for _, s := range files {
		Convey("Primitive: write and read files of various sizes and types", t, func() {

			inputFile := "./data/" + s.Name()
			file, err := os.Open(inputFile) // For read access.
			if err != nil {
				log.Fatal(err)
			}
			stat, err := file.Stat()
			if err != nil {
				log.Fatal(err)
			}
			primitive := mode.Primitive{"", "", int(stat.Size()), 0, 0, "", "", ""}
			reader := bufio.NewReader(file)
			primitive.Length = int(stat.Size())
			err = primitive.Make(reader)
			if err != nil {
				log.Fatal(err)
			}
			Convey("Given a Reader, make a new primitive", func() {
				Convey("the value of size should be the same as the original file", func() {
					So(primitive.Length, ShouldEqual, stat.Size())
				})
			})
			Convey("Given a Writer, write primitive to a file", func() {
				outputFile := "./data/" + primitive.Id
				file, err := os.Create(outputFile) // For write access.
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()

				var readPrimitive mode.Primitive
				readPrimitive.Id = primitive.Id
				readFile := bufio.NewWriter(file)
				e := readPrimitive.Stream(readFile)
				file.Sync()

				So(e, ShouldEqual, nil)
				So(readPrimitive.Length, ShouldEqual, stat.Size())

			})
			Convey("Delete the primitive", func() {

				e := primitive.Destroy()
				So(e, ShouldEqual, nil)
				So(primitive.Id, ShouldEqual, "")

			})
			Convey("Try to read the primitive that was just deleted", func() {
				// returns zero bytes read, meaning the primitive does not exist
				var readPrimitive mode.Primitive
				readPrimitive.Id = primitive.Id

				e := readPrimitive.Find()
				So(e.Error(), ShouldEqual, "Invalid primtive id:")
				So(readPrimitive.Length, ShouldEqual, 0)
				So(readPrimitive.Id, ShouldEqual, primitive.Id)
			})

		})
	}
}
