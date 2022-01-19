package mistconnector

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
)

type MistOptional struct {
	Name    string `json:"name"`
	Help    string `json:"help"`
	Option  string `json:"option,omitempty"`
	Default string `json:"default,omitempty"`
	Type    string `json:"type,omitempty"`
}

type MistConfig struct {
	Name         string                  `json:"name"`
	Description  string                  `json:"desc"`
	FriendlyName string                  `json:"friendly"`
	Optional     map[string]MistOptional `json:"optional,omitempty"`
	Version      string                  `json:"version,omitempty"`
}

func isIntType(value string) bool {
	if _, err := strconv.Atoi(value); err == nil {
		return true
	}
	return false
}

func isBoolType(value string) bool {
	if _, err := strconv.ParseBool(value); err == nil {
		return true
	}
	return false
}

func PrintMistConfigJson(name, description, friendlyName, version string, flagSet *flag.FlagSet) {
	data := MistConfig{
		Name:         name,
		Version:      version,
		Description:  description,
		FriendlyName: friendlyName,
		Optional:     make(map[string]MistOptional),
	}
	flagSet.VisitAll(func(f *flag.Flag) {
		var flagType string = "str"
		if len(f.DefValue) > 0 {
			if isIntType(f.DefValue) {
				flagType = "uint"
			}
			if isBoolType(f.DefValue) {
				flagType = ""
			}
		}
		data.Optional[f.Name] = MistOptional{
			Name:    f.Name,
			Help:    f.Usage,
			Option:  fmt.Sprintf("-%s", f.Name),
			Default: f.DefValue,
			Type:    flagType,
		}
	})
	b, _ := json.Marshal(data)
	os.Stdout.Write(b)
}
