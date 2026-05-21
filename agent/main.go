package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type Task struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type Result struct {
	TaskID  string `json:"task_id"`
	Success bool   `json:"success"`
	Output  string `json:"output"`
}

type RoutePayload struct {
	OrderID     string `json:"order_id"`
	Destination string `json:"destination"`
}

var processedTasksCounter uint64

func main() {
	logFile, err := os.OpenFile("agent.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Ошибка создания файла логов: %v", err)
	}
	defer logFile.Close()

	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к NATS: %v", err)
	}
	defer nc.Close()

	log.Println("[INFO] Агент планирования маршрутов успешно запущен...")

	_, err = nc.QueueSubscribe("tasks.process", "route_planner_group", func(m *nats.Msg) {
		var task Task
		if err := json.Unmarshal(m.Data, &task); err != nil {
			log.Printf("[ERROR] Ошибка десериализации задачи: %v", err)
			return
		}

		if task.Type != "route_planning" {
			return
		}

		log.Printf("[INFO] Получена задача %s для обработки", task.ID)

		output := processRouteTask(task)

		atomic.AddUint64(&processedTasksCounter, 1)
		log.Printf("[INFO] Задач обработано текущим агентом: %d", atomic.LoadUint64(&processedTasksCounter))

		result := Result{
			TaskID:  task.ID,
			Success: true,
			Output:  output,
		}

		response, _ := json.Marshal(result)
		nc.Publish("tasks.completed", response)
		log.Printf("[INFO] Результат по задаче %s отправлен в NATS", task.ID)
	})

	if err != nil {
		log.Fatalf("Ошибка подписки: %v", err)
	}

	select {}
}

func processRouteTask(task Task) string {
	var p RoutePayload
	_ = json.Unmarshal([]byte(task.Payload), &p)

	time.Sleep(500 * time.Millisecond)

	res := map[string]interface{}{
		"route_id":     "route-" + task.ID[:8],
		"distance_km":  12.4,
		"duration_min": 35,
		"status":       "optimized",
	}
	bytes, _ := json.Marshal(res)
	return string(bytes)
}
