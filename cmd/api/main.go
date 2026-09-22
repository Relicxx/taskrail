package main

import (
	"github.com/Relicxx/taskrail/internal/app"
	"log"
)

func main() {
	if e := app.Run("api"); e != nil {
		log.Fatal(e)
	}
}
