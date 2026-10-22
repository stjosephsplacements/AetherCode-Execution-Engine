package worker

import (
	"testing"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

func TestJudgeFloatRejectsNaN(t *testing.T) {
	cases := []struct {
		name, expected, actual string
		want                   model.Verdict
	}{
		{"NaN actual", "3.14", "NaN", model.VerdictWrongAnswer},
		{"NaN expected", "NaN", "3.14", model.VerdictWrongAnswer},
		{"NaN both", "NaN", "NaN", model.VerdictWrongAnswer},
		{"NaN later token", "1.0 2.0", "1.0 NaN", model.VerdictWrongAnswer},
		{"positive infinity matches", "+Inf", "+Inf", model.VerdictAccepted},
		{"opposite infinities differ", "+Inf", "-Inf", model.VerdictWrongAnswer},
		{"normal float still matches", "1.0", "1.0000001", model.VerdictAccepted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := JudgeOutput("float", tc.expected, tc.actual)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
