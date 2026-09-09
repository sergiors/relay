package logging

import (
	"fmt"
	"strings"
)

// Fields renders key/value pairs as " key=value" pairs for appending to the end
// of a log message. Values containing spaces or tabs are Go-quoted; empty
// string values are skipped; a trailing odd argument is ignored.
func Fields(kv ...any) string {
	var b strings.Builder
	for i := 0; i+1 < len(kv); i += 2 {
		name, ok := kv[i].(string)
		if !ok {
			continue
		}
		value := fmt.Sprintf("%v", kv[i+1])
		if value == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(name)
		b.WriteByte('=')
		if strings.ContainsAny(value, " \t") {
			fmt.Fprintf(&b, "%q", value)
		} else {
			b.WriteString(value)
		}
	}
	return b.String()
}
