package main

import (
	"encoding/json"
	"testing"
)

func TestProcessRouteTask(t *testing.T) {
	mockTask := Task{
		ID:      "test-uuid-12345",
		Type:    "route_planning",
		Payload: `{"order_id":"abc","destination":"Test Street"}`,
	}

	output := processRouteTask(mockTask)

	var result map[string]interface{}
	err := json.Unmarshal([]byte(output), &result)
	if err != nil {
		t.Fatalf("Выходные данные агента не являются валидным JSON: %v", err)
	}

	if result["status"] != "optimized" {
		t.Errorf("Ожидался статус 'optimized', получили: %v", result["status"])
	}

	if result["distance_km"] != 12.4 {
		t.Errorf("Неверная дистанция маршрута")
	}
}
