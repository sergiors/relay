package cli

import (
	"fmt"
	"io"
	"text/tabwriter"
)

// output remembers the first write error so a renderer can batch formatted
// writes and report one failure. Tabwriters write through it, so buffered and
// Flush failures are captured too.
type output struct {
	w   io.Writer
	err error
}

func newOutput(w io.Writer) *output {
	return &output{w: w}
}

// Write implements io.Writer for the tabwriters created by table.
func (o *output) Write(p []byte) (int, error) {
	if o.err != nil {
		return 0, o.err
	}
	n, err := o.w.Write(p)
	if err != nil {
		o.err = err
	}
	return n, err
}

func (o *output) printf(format string, args ...any) {
	if o.err != nil {
		return
	}
	_, o.err = fmt.Fprintf(o.w, format, args...)
}

func (o *output) line(format string, args ...any) {
	o.printf(format+"\n", args...)
}

// blank separates sections.
func (o *output) blank() {
	o.printf("\n")
}

func (o *output) table(padding int) *tabwriter.Writer {
	return tabwriter.NewWriter(o, 0, 4, padding, ' ', 0)
}

func (o *output) flush(tw *tabwriter.Writer) {
	if err := tw.Flush(); err != nil && o.err == nil {
		o.err = err
	}
}
