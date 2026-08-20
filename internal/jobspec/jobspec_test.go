package jobspec

import "testing"

func TestParse(t *testing.T) {
	spec, err := Parse([]byte(`
jobs:
  test:
    image: node:22
    commands:
      - npm install
      - npm test
    env:
      CI: "true"
`))
	if err != nil {
		t.Fatal(err)
	}
	job := spec.Primary()
	if job.Image != "node:22" {
		t.Errorf("image = %q", job.Image)
	}
	if len(job.Commands) != 2 {
		t.Errorf("commands = %v", job.Commands)
	}
	if job.Env["CI"] != "true" {
		t.Errorf("env = %v", job.Env)
	}
	if job.Name != "test" {
		t.Errorf("name = %q", job.Name)
	}
}

func TestPrimaryIsDeterministic(t *testing.T) {
	spec, err := Parse([]byte(`
jobs:
  zeta:
    image: alpine
    commands: ["echo z"]
  alpha:
    image: alpine
    commands: ["echo a"]
`))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if spec.Primary().Name != "alpha" {
			t.Fatal("Primary() must be stable across map iterations")
		}
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"no jobs":     "jobs: {}",
		"no commands": "jobs:\n  test:\n    image: alpine\n",
		"bad yaml":    "jobs: [",
	}
	for name, in := range cases {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
