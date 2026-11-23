package main

// Expected behavior:
//   - Decides whether a burst of new messages is worth an SMS, by asking TypeSafe's Jev (a
//     judgment model) one yes/no question with the recent chat as context. Jev answers with the
//     probability of yes; the scheduler forwards above forwardThreshold.
//   - The question lives in judge_question.json (embedded at build time), so it can be tuned and
//     replayed against real chats without touching code.
//   - The state Jev sees is the chat's last messages from forwarder.db: the new ones marked
//     is_new, earlier forwards marked was_sent_to_dumb_phone, your own messages as from "me", and
//     how many minutes ago you last wrote in the chat.
//
// Gotchas:
//   - Jev is not a chat model: it gets a JSON state and typed questions, never a prompt.
//   - The scheduler forwards anyway when Jev fails: a missed message is worse than an extra SMS.

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const jevURL = "https://api.typesafe.ai/v1/systemone"
const jevModel = "jev-latest"
const jevRequestTimeout = 30 * time.Second
const forwardThreshold = 0.5
const judgeContextWindow = 12 * time.Hour
const judgeContextLimit = 20
const judgeQuestionName = "should_forward"

//go:embed judge_question.json
var judgeQuestion json.RawMessage

// ForwardJudge returns the probability (0–1) that the new messages in the state deserve an SMS.
type ForwardJudge interface {
	ForwardProbability(ctx context.Context, state JudgeState) (float64, error)
}

type JudgeState struct {
	Chat                      string         `json:"chat"`
	Now                       string         `json:"now"`
	Messages                  []JudgeMessage `json:"messages"`
	MinutesSinceUserWroteHere *int           `json:"minutes_since_user_wrote_here,omitempty"`
}

type JudgeMessage struct {
	Time               string `json:"time"`
	From               string `json:"from"`
	Text               string `json:"text"`
	IsNew              bool   `json:"is_new,omitempty"`
	WasSentToDumbPhone bool   `json:"was_sent_to_dumb_phone,omitempty"`
}

type jevJudge struct {
	url        string
	apiKey     string
	httpClient *http.Client
}

// Lifecycle
// ---------------------------------------------------------------------------

func newJevJudge(apiKey string) *jevJudge {
	return &jevJudge{url: jevURL, apiKey: apiKey, httpClient: &http.Client{Timeout: jevRequestTimeout}}
}

func (judge *jevJudge) ForwardProbability(ctx context.Context, state JudgeState) (float64, error) {
	payload, err := json.Marshal(map[string]any{
		"model":     jevModel,
		"state":     state,
		"questions": map[string]json.RawMessage{judgeQuestionName: judgeQuestion},
	})
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, judge.url, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+judge.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := judge.httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("jev %d: %s", response.StatusCode, body)
	}
	return parseJevProbability(body)
}

// State
// ---------------------------------------------------------------------------

// judgeState builds what Jev sees: history is the chat's recent rows (burst included), oldest first.
func judgeState(chat ChatInfo, history []Message, burst []Message, now time.Time) JudgeState {
	isNew := map[string]bool{}
	for _, message := range burst {
		isNew[message.MessageID] = true
	}
	state := JudgeState{Chat: chatLabel(chat), Now: judgeTime(now)}
	for _, message := range history {
		judgeMessage := JudgeMessage{Time: judgeTime(message.ReceivedAt), From: message.SenderName, Text: message.Text}
		switch {
		case isNew[message.MessageID]:
			judgeMessage.IsNew = true
		case message.Status == StatusMine:
			judgeMessage.From = "me"
			minutes := int(now.Sub(message.ReceivedAt).Minutes())
			state.MinutesSinceUserWroteHere = &minutes
		case message.Status == StatusSent:
			judgeMessage.WasSentToDumbPhone = true
		}
		state.Messages = append(state.Messages, judgeMessage)
	}
	return state
}

func chatLabel(chat ChatInfo) string {
	if chat.IsGroup {
		return fmt.Sprintf("group chat %q", chat.GroupName)
	}
	return "direct chat"
}

func judgeTime(moment time.Time) string {
	return moment.Local().Format("Mon 15:04")
}

// Response
// ---------------------------------------------------------------------------

func parseJevProbability(body []byte) (float64, error) {
	var response struct {
		Answers map[string]struct {
			Noul *float64 `json:"noul"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return 0, fmt.Errorf("jev response: %w", err)
	}
	answer := response.Answers[judgeQuestionName].Noul
	if answer == nil || *answer < 0 || *answer > 1 {
		return 0, fmt.Errorf("jev response without a probability: %s", body)
	}
	return *answer, nil
}
