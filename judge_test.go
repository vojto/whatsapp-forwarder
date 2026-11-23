package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJevJudgeSendsStateAndQuestionAndReadsProbability(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		if incoming.Header.Get("Authorization") != "Bearer key" {
			t.Errorf("authorization = %q", incoming.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(incoming.Body)
		json.Unmarshal(body, &request)
		writer.Write([]byte(`{"model":"jev-1","answers":{"should_forward":{"type":"noul","noul":0.83}},"usage":{}}`))
	}))
	defer server.Close()
	judge := newJevJudge("key")
	judge.url = server.URL

	probability, err := judge.ForwardProbability(context.Background(), JudgeState{Chat: "direct chat", Messages: []JudgeMessage{{From: "Lenka", Text: "Obed?", IsNew: true}}})
	if err != nil || probability != 0.83 {
		t.Fatalf("probability = %v, err = %v", probability, err)
	}
	question := request["questions"].(map[string]any)[judgeQuestionName].(map[string]any)
	if request["model"] != jevModel || question["type"] != "noul" {
		t.Fatalf("request = %v", request)
	}
	messages := request["state"].(map[string]any)["messages"].([]any)
	if first := messages[0].(map[string]any); first["is_new"] != true || first["text"] != "Obed?" {
		t.Fatalf("state messages = %v", messages)
	}
}

func TestJevJudgeErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"http error", http.StatusUnauthorized, `{"detail":"bad key"}`},
		{"missing answer", http.StatusOK, `{"answers":{}}`},
		{"out of range", http.StatusOK, `{"answers":{"should_forward":{"noul":1.5}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
				writer.WriteHeader(test.status)
				writer.Write([]byte(test.body))
			}))
			defer server.Close()
			judge := newJevJudge("key")
			judge.url = server.URL
			if _, err := judge.ForwardProbability(context.Background(), JudgeState{}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
