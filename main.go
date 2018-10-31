package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"reflect"
	"github.com/pborman/uuid"
)

const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	// Needs to update
    //PROJECT_ID = "around-xxx"
	//BT_INSTANCE = "around-post"
	// Needs to update this URL if you deploy it to cloud.
	ES_URL = "http://35.237.131.231:9200"

)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
}


func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}


	fmt.Println("started-service")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func handlerPost(w http.ResponseWriter, r *http.Request) {
    // Parse from body of request to get a json object.
	fmt.Println("Received one post request")
    decoder := json.NewDecoder(r.Body)
    var p Post
    if err := decoder.Decode(&p); err != nil {
        panic(err)
        return
    }
	fmt.Printf("Post received2: %s\n", p.Message)
	fmt.Fprintf(w, "Post received3: %s\n", p.Message)
	
	id := uuid.New()
	saveToES(&p, id)

}

func saveToES(p *Post, id string) {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL),
		elastic.SetSniff(false))

	if err != nil {
		panic(err)
	}

	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Post is saved to index: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search:\n")
	  /*
      lat := r.URL.Query().Get("lat")
      lon := r.URL.Query().Get("lon")
	  
	  fmt.Printf("Search received: %s %s", lat, lon)
	  fmt.Fprintf(w, "Search received: %s %s", lat, lon)*/
	  
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
    lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
    // range is optional 
    ran := DISTANCE 
    if val := r.URL.Query().Get("range"); val != "" { 
        ran = val + "km" 
    }

	fmt.Printf("Search received: %f %f %s \n", lat, lon, ran)
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	  
	if err != nil {
		panic(err)
		return
	}

	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)
	fmt.Printf("q: T: %T    v: %v     d: %d    s: %s  \n", q, q, q, q)
	searchResult, err := client.Search().
		Index(INDEX). // "around"
		Query(q).
		Pretty(true).
		Do()

	fmt.Printf("searchResult: T: %T    v: %v     d: %d    s: %s \n", searchResult, searchResult, searchResult, searchResult)

	if err != nil {
		panic(err)
	}

	fmt.Println("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts\n", searchResult.TotalHits())

	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // java: instance of
		fmt.Printf("item: T: %T    v: %v     d: %d    s: %s \n", item, item, item, item)
		p := item.(Post) // type conversion in java: p = (Post) item
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", 
			p.User, p.Message, p.Location.Lat, p.Location.Lon)
		ps = append(ps, p)
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)

}



