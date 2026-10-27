package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	PasswordEnv              string     `yaml:"password_env"`
	Listen                   string     `yaml:"listen"`
	PollInterval             string     `yaml:"poll_interval"`
	RequestTimeout           string     `yaml:"request_timeout"`
	ReconnectInterval        string     `yaml:"reconnect_interval"`
	Instances                []Instance `yaml:"instances"`
	poll, timeout, reconnect time.Duration
}
type Instance struct {
	Name        string   `yaml:"name"`
	URLs        []string `yaml:"urls"`
	URL         string   `yaml:"url"`
	PasswordEnv string   `yaml:"password_env"`
	password    string
}

func loadConfig(path string) (Config, error) {
	c := Config{Listen: ":9407", PollInterval: "5s", RequestTimeout: "3s", ReconnectInterval: "5s"}
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".yml" && ext != ".yaml" {
		return c, fmt.Errorf("configuration file must use .yml or .yaml")
	}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("config must contain one YAML document")
	}
	for _, v := range []struct {
		name, raw string
		dst       *time.Duration
	}{
		{"poll_interval", c.PollInterval, &c.poll}, {"request_timeout", c.RequestTimeout, &c.timeout}, {"reconnect_interval", c.ReconnectInterval, &c.reconnect},
	} {
		duration, e := time.ParseDuration(v.raw)
		if e != nil || duration <= 0 {
			return c, fmt.Errorf("%s must be a positive duration", v.name)
		}
		*v.dst = duration
	}
	if c.Listen == "" || len(c.Instances) == 0 {
		return c, fmt.Errorf("listen and at least one instance are required")
	}
	seen := map[string]bool{}
	for i := range c.Instances {
		v := &c.Instances[i]
		if v.Name == "" || seen[v.Name] {
			return c, fmt.Errorf("instance names must be nonempty and unique")
		}
		seen[v.Name] = true

		if v.URL != "" && len(v.URLs) > 0 {
			return c, fmt.Errorf("instance %q: use urls or url, not both", v.Name)
		}
		if v.URL != "" {
			v.URLs = []string{v.URL}
		}
		if len(v.URLs) == 0 {
			return c, fmt.Errorf("instance %q requires at least one WebSocket URL", v.Name)
		}
		endpoints := map[string]bool{}
		for _, address := range v.URLs {
			u, e := url.Parse(address)
			if e != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return c, fmt.Errorf("instance %q requires ws:// or wss:// URLs without credentials, query or fragment", v.Name)
			}
			if endpoints[address] {
				return c, fmt.Errorf("instance %q has duplicate URLs", v.Name)
			}
			endpoints[address] = true
		}

		if v.PasswordEnv == "" {
			v.PasswordEnv = c.PasswordEnv
		}
		if v.PasswordEnv != "" {
			var ok bool
			v.password, ok = os.LookupEnv(v.PasswordEnv)
			if !ok || v.password == "" {
				return c, fmt.Errorf("instance %q: password environment variable %q is missing or empty", v.Name, v.PasswordEnv)
			}
		}
	}
	return c, nil
}

// endpoints returns the configured alternatives or the single-address shorthand.
func (i Instance) endpoints() []string {
	if len(i.URLs) > 0 {
		return i.URLs
	}
	if i.URL != "" {
		return []string{i.URL}
	}
	return nil
}
