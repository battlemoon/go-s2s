package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/coccyx/go-s2s/s2s"
)

// convertEvent maps a map[string]interface{} to map[string]string and also
// maps fields in Splunk's HEC format to Splunk's normal field names
func convertEvent(origEvent map[string]interface{}) map[string]string {
	ret := map[string]string{}
	for k, v := range origEvent {
		if k == "fields" {
			fieldsRet := convertEvent(v.(map[string]interface{}))
			for k, v := range fieldsRet {
				ret[k] = v
			}
			continue
		}
		if k == "event" {
			ret["_raw"] = v.(string)
			continue
		}
		if k == "time" || k == "_time" {
			ret["_time"] = fmt.Sprintf("%.3f", v)
			continue
		}
		if k == "_subsecond" {
			continue
		}
		switch v.(type) {
		case string:
			ret[k] = v.(string)
		case float64:
			ret[k] = fmt.Sprintf("%f", v)
		case int:
			ret[k] = fmt.Sprintf("%d", v)
		default:
			ret[k] = fmt.Sprintf("%v", v)
		}
	}
	return ret
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: go-s2s <host>:<port>\n\n  Expects a newline delimited JSON documents on stdin with Splunk standard fields. Each row\n  should have at least index, host, source, sourcetype, _time and _raw.\n")
		os.Exit(1)
	}

	s, err := s2s.NewS2S([]string{os.Args[1]}, 0)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("error connecting to splunk: %v", err))
		os.Exit(1)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		event := map[string]interface{}{}
		err := json.Unmarshal([]byte(scanner.Text()), &event)
		if err != nil {
			continue // Ignore bad documents
		}
		newEvent := convertEvent(event)
		fmt.Printf("%v\n", newEvent)
		_, err = s.Send(newEvent)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("error sending event: %v\n", err))
			os.Exit(1)
		}
	}
}
