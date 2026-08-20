// Package jobspec parses forgerun.yml from a checked-out repository.
//
//	jobs:
//	  test:
//	    image: node:22
//	    commands:
//	      - npm install
//	      - npm test
//
// The MVP executes exactly one job (the first one, or "test" if present), but the
// file format is already a map of jobs so multi-job support is additive later.
package jobspec

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

type Spec struct {
	Jobs map[string]Job `yaml:"jobs"`
}

type Job struct {
	Name     string            `yaml:"-"`
	Image    string            `yaml:"image"`
	Commands []string          `yaml:"commands"`
	Env      map[string]string `yaml:"env"`
	Labels   []string          `yaml:"labels"`
}

func Parse(data []byte) (*Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse forgerun.yml: %w", err)
	}
	if len(s.Jobs) == 0 {
		return nil, fmt.Errorf("forgerun.yml defines no jobs")
	}
	for name, j := range s.Jobs {
		if len(j.Commands) == 0 {
			return nil, fmt.Errorf("job %q has no commands", name)
		}
		j.Name = name
		s.Jobs[name] = j
	}
	return &s, nil
}

func ParseFile(path string) (*Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Primary returns the single job the MVP runs: "test" when defined, otherwise the
// alphabetically first job so the choice is deterministic across runs.
func (s *Spec) Primary() Job {
	if j, ok := s.Jobs["test"]; ok {
		return j
	}
	names := make([]string, 0, len(s.Jobs))
	for n := range s.Jobs {
		names = append(names, n)
	}
	sort.Strings(names)
	return s.Jobs[names[0]]
}
