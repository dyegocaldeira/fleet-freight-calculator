package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dyegocaldeira/fleet-freight-calculator/internal/freight/entity"
	"github.com/dyegocaldeira/fleet-freight-calculator/internal/freight/infra/repository"
	"github.com/dyegocaldeira/fleet-freight-calculator/internal/freight/usecase"
	"github.com/dyegocaldeira/fleet-freight-calculator/pkg/kafka"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/routes?parseTime=true")
	if err != nil {
		panic(err)
	}

	defer db.Close()

	msgChan := make(chan *ckafka.Message)
	topics := []string{"routes"}
	servers := "localhost:9094"

	go kafka.Consume(topics, servers, msgChan)

	repository := repository.NewRouteRepositoryMysql(db)
	freight := entity.NewFreight(10)
	createRouteUseCase := usecase.NewCreateRouteUseCase(repository, freight)
	changeRouteStatusUseCase := usecase.NewChangeRouteStatusUseCase(repository)

	for msg := range msgChan {
		input := usecase.CreateRouteInput{}
		json.Unmarshal(msg.Value, &input)

		switch input.Event {
		case "RouteCreated":
			output, err := createRouteUseCase.Execute(input)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(output)

		case "RouteUpdated":
			input := usecase.ChangeRouteStatusInput{}
			json.Unmarshal(msg.Value, &input)
			output, err := changeRouteStatusUseCase.Execute(input)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(output)
		}
	}
}
