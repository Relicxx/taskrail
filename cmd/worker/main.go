package main

import (
	"log"

	"github.com/Relicxx/taskrail/internal/app"
)

func main() {
	if e := app.Run("worker"); e != nil {
		log.Fatal(e)
	}
}
