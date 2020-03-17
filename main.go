package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type Weather struct {
	Time        string `json:"time"`
	Temperature string `json:"temperature"`
}

func main() {

	client, err := gorm.Open("mysql", "root:root@tcp([mysql]:3306)/Weather?charset=utf8mb4&parseTime=true")
	if err != nil {
		log.Fatal(err.Error())
	}

	client.AutoMigrate(&Weather{})

	defer client.Close()

	account_json, err := ioutil.ReadFile("data.json")
	if err != nil {
		log.Fatal(err.Error())
	}

	var data []Weather
	if err := json.Unmarshal(account_json, &data); err != nil {
		log.Fatal(err)
	}

	done := make(chan interface{})
	defer close(done)

	hertbeat, results, errch := insert(done, data, client)

	<-hertbeat

	i := 0

	for {
		select {
		case r, ok := <-results:
			if ok == false {
				return
			} else if Weather := data[i]; Weather != r {
				log.Fatal("MismatchingError", Weather)
			}
			i++
		case <-errch:
			log.Fatal(errch)
		case <-hertbeat:
		case <-time.After(5 * time.Second):
			log.Fatal("imeout")
		}
	}

}

func insert(done <-chan interface{}, data []Weather, client *gorm.DB) (<-chan interface{}, <-chan Weather, <-chan error) {
	hertbeat := make(chan interface{}, 1)
	weatherch := make(chan Weather)
	errch := make(chan error)

	go func() {
		defer close(hertbeat)
		defer close(weatherch)

		beat := time.Tick(time.Second)

	Loop:
		for _, name := range data {
			for {

				select {
				case <-done:
					return
				case <-beat:
					select {
					case hertbeat <- struct{}{}:
					default:
					}
				case weatherch <- name:
					client := client.Create(&name)
					if client.Error != nil {
						errch <- client.Error
					}

					continue Loop
				}
			}
		}
	}()

	return hertbeat, weatherch, errch
}
