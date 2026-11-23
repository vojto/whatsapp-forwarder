package main

// Expected behavior:
//   - Evals for the forward judge: runs every case in eval/cases.json through the real Jev API
//     with the current judge_question.json and fails for each case it gets wrong.
//   - Only runs with JEV_EVAL=1 and TYPESAFE_API_KEY set (it costs API calls and needs network):
//     JEV_EVAL=1 go test -run Eval -v
//
// Gotchas:
//   - eval/cases.json holds real situations rewritten in English with made-up people, so it is
//     safe to commit. Each case is a JudgeState as the scheduler builds it, plus the right answer.
//   - Never copy a case's wording into judge_question.json: the question keeps general rules, so
//     the evals measure whether Jev generalizes rather than whether it matches its own examples.
//   - Jev's answers wobble a little between calls, so a case near 0.5 can flip.

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/joho/godotenv"
)

type judgeEvalCase struct {
	Name          string     `json:"name"`
	ShouldForward bool       `json:"should_forward"`
	State         JudgeState `json:"state"`
}

func TestEvalForwardJudge(t *testing.T) {
	if os.Getenv("JEV_EVAL") == "" {
		t.Skip("set JEV_EVAL=1 to run the forward judge evals against Jev")
	}
	godotenv.Load()
	apiKey := os.Getenv("TYPESAFE_API_KEY")
	if apiKey == "" {
		t.Fatal("TYPESAFE_API_KEY is not set")
	}
	content, err := os.ReadFile("eval/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []judgeEvalCase
	if err := json.Unmarshal(content, &cases); err != nil {
		t.Fatal(err)
	}

	judge := newJevJudge(apiKey)
	probabilities := make([]float64, len(cases))
	errs := make([]error, len(cases))
	var waitGroup sync.WaitGroup
	for index, evalCase := range cases {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			probabilities[index], errs[index] = judge.ForwardProbability(context.Background(), evalCase.State)
		}()
	}
	waitGroup.Wait()

	passed := 0
	for index, evalCase := range cases {
		if errs[index] != nil {
			t.Errorf("%s: %v", evalCase.Name, errs[index])
			continue
		}
		isForwarded := probabilities[index] > forwardThreshold
		if isForwarded == evalCase.ShouldForward {
			passed++
			t.Logf("ok   %.2f %s", probabilities[index], evalCase.Name)
			continue
		}
		t.Errorf("FAIL %.2f %s (want forward=%v)", probabilities[index], evalCase.Name, evalCase.ShouldForward)
	}
	t.Logf("%d/%d passed", passed, len(cases))
}
