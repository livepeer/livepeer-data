package mistconnector

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
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
	Required     map[string]MistOptional `json:"required,omitempty"`
	Version      string                  `json:"version,omitempty"`
}

func isBoolType(value string) bool {
	boolValues := []string{"true", "false"}
	for _, bv := range boolValues {
		if strings.ToLower(value) == bv {
			return true
		}
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
		flagType := "str"
		if len(f.DefValue) > 0 {
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
