package runtime

import "testing"

func TestHandlerContextNormalizedUsesExecutingApp(t *testing.T) {
	hc := (HandlerContext{
		RequestID: "request",
		Attempt:   1,
		App:       "stale-app",
		Handler:   "stale.handler",
	}).normalized("executing-app", "executing.handler")

	if hc.App != "executing-app" {
		t.Errorf("app = %q, want executing-app", hc.App)
	}
	if hc.Handler != "executing.handler" {
		t.Errorf("handler = %q, want executing.handler", hc.Handler)
	}
}
