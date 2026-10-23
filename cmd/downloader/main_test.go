package main

import (
	"flag"
	"reflect"
	"testing"
)

func TestParseInterspersed(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		wantPositional []string
		wantWorkers    int
		wantAdaptive   bool
	}{
		{"flags before url", []string{"-w", "8", "-a", "http://x/f"}, []string{"http://x/f"}, 8, true},
		{"flags after url", []string{"http://x/f", "-w", "8", "-a"}, []string{"http://x/f"}, 8, true},
		{"flags around url", []string{"-w", "8", "http://x/f", "-a"}, []string{"http://x/f"}, 8, true},
		{"no flags", []string{"http://x/f"}, []string{"http://x/f"}, 4, false},
		{"no args", nil, nil, 4, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			workers := fs.Int("w", 4, "")
			adaptive := fs.Bool("a", false, "")

			got := parseInterspersed(fs, tt.args)

			if !reflect.DeepEqual(got, tt.wantPositional) {
				t.Errorf("positional = %v, want %v", got, tt.wantPositional)
			}
			if *workers != tt.wantWorkers {
				t.Errorf("workers = %d, want %d", *workers, tt.wantWorkers)
			}
			if *adaptive != tt.wantAdaptive {
				t.Errorf("adaptive = %v, want %v", *adaptive, tt.wantAdaptive)
			}
		})
	}
}
