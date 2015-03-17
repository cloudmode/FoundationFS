package main

import (
	//"crypto/md5"
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/FoundationFS/mode"
	"html/template"
	//	"io"
	"log"
	"net/http"
	"os"
	//"time"
)

//Compile templates on start
var templates = template.Must(template.ParseFiles("tmpl/upload.html"))

//Display the named template
func display(w http.ResponseWriter, tmpl string, data interface{}) {
	templates.ExecuteTemplate(w, tmpl+".html", data)
}

// upload logic
func upload(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	if r.Method == "GET" {
		display(w, "upload", nil)
	} else {
		//parse the multipart form in the request
		err := r.ParseMultipartForm(100000)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		//get a ref to the parsed multipart form
		m := r.MultipartForm

		//get the *fileheaders
		files := m.File["myfiles"]
		p := new(mode.Primitive)
		for i, _ := range files {
			//for each fileheader, get a handle to the actual file
			file, err := files[i].Open()
			defer file.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			stat, err := file.(*os.File).Stat()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			//create destination file making sure the path is writeable.
			header := files[i].Header
			fmt.Printf("file:%d %T %#v\n", i, file, file)
			fmt.Printf("files[i]:type:\n%T \n%#v\n", header.Get("Content-Type"), header.Get("Content-Type"))
			//dst, err := os.Create("/tmp/" + files[i].Filename)

			p.Name = files[i].Filename
			p.MimeType = header.Get("Content-Type")
			p.Length = int(stat.Size())

			reader := bufio.NewReader(file)
			err = p.Make(reader)

			//defer dst.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			//copy the uploaded file to the destination file
			//if _, err := io.Copy(dst, file); err != nil {
			//	http.Error(w, err.Error(), http.StatusInternalServerError)
			//	return
			//}

		}
		js, err := json.Marshal(p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

func download(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// check to see if id is in URL
		// otherwise return error
		id := r.FormValue("id")
		if id == "" || len(id) != 32 {
			w.Header().Set("Content-Type", "application/json")
			e := map[string]string{"success": "false", "error": "missing or invalid id in query"}
			js, err := json.Marshal(e)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
		}
		writer := bufio.NewWriter(w)
		p := new(mode.Primitive)
		p.Id = id
		err := p.Stream(writer)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	} else {
		http.Error(w, "method not supported", http.StatusInternalServerError)
		return
	}
	return
}

func main() {
	//http.HandleFunc("/", sayhelloName) // setting router rule
	//http.HandleFunc("/login", login)
	http.HandleFunc("/upload", upload)
	http.HandleFunc("/download", download)

	err := http.ListenAndServe(":9090", nil) // setting listening port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
