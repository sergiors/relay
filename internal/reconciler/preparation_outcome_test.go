package reconciler

import (
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// testPreparedApp is a registry entry holding a usable active image, built from
// the package's standard fixtures (the no-service template and the fake
// builder).
func testPreparedApp(fingerprint string) *runner.PreparedApp {
	fn := app.App{Name: "outcome-app", Dir: ".", Template: mustParse(template)}
	return runner.NewPrepared(fn, &runtime.Prepared{
		Name:        fn.Name,
		Image:       "img-" + fn.Name,
		Fingerprint: fingerprint,
	}, &fakeBuilder{})
}

// TestPreparationOutcomeClassifiesFailedDesiredFingerprint pins the classifier
// behind the `App: preparing` line: a persisted desired generation whose last
// reconcile FAILED is compared against the fingerprint this pass resolved.
// Re-presenting that same identity is a RETRY of the failed attempt; resolving
// a different identity is a NEW GENERATION. Both must hold whether an older
// generation is still serving (cur != nil) or nothing is registered at all
// (cur == nil, the first build of an app whose earlier attempt failed), and
// only when the recorded desired fingerprint is non-empty.
func TestPreparationOutcomeClassifiesFailedDesiredFingerprint(t *testing.T) {
	// given a persisted row whose LAST reconcile failed on a specific desired
	// fingerprint, and a row that failed without recording one
	failedObservation := &state.AppObservation{
		Found: true,
		Detail: state.Detail{
			LastReconcileStatus: state.ReconcileFailed,
			DesiredFingerprint:  "failed-fp-1111",
		},
	}
	failedWithoutFingerprint := &state.AppObservation{
		Found: true,
		Detail: state.Detail{
			LastReconcileStatus: state.ReconcileFailed,
		},
	}
	succeededObservation := &state.AppObservation{
		Found: true,
		Detail: state.Detail{
			LastReconcileStatus: state.ReconcileSuccess,
		},
	}

	const activeFingerprint = "active-fp-0000"
	activeGeneration := testPreparedApp(activeFingerprint)

	tests := []struct {
		name     string
		cur      *runner.PreparedApp
		observed *state.AppObservation
		known    string
		hasKnown bool
		desired  string
		want     string
	}{
		// No registry entry at all: the earlier attempt left nothing serving.
		{
			name:     "no active generation retries the same failed desired fingerprint",
			cur:      (*runner.PreparedApp)(nil),
			observed: failedObservation,
			desired:  "failed-fp-1111",
			want:     "retry",
		},
		{
			name:     "no active generation opens a new generation after the failure",
			cur:      (*runner.PreparedApp)(nil),
			observed: failedObservation,
			desired:  "next-fp-2222",
			want:     "new_generation",
		},
		{
			name:     "no active generation without a recorded failure is a discovery",
			cur:      (*runner.PreparedApp)(nil),
			observed: nil,
			desired:  "first-fp-0000",
			want:     "discovered",
		},
		{
			name:     "a recorded success never classifies as a failure retry",
			cur:      (*runner.PreparedApp)(nil),
			observed: succeededObservation,
			desired:  "next-fp-2222",
			want:     "discovered",
		},
		{
			name:     "a failed row without a desired fingerprint is not a retry",
			cur:      (*runner.PreparedApp)(nil),
			observed: failedWithoutFingerprint,
			desired:  "next-fp-2222",
			want:     "discovered",
		},

		// An older generation still serves while the failed desired one is
		// retried or replaced.
		{
			name:     "serving generation retries the same failed desired fingerprint",
			cur:      activeGeneration,
			observed: failedObservation,
			known:    activeFingerprint,
			hasKnown: true,
			desired:  "failed-fp-1111",
			want:     "retry",
		},
		{
			name:     "serving generation opens a new generation after the failure",
			cur:      activeGeneration,
			observed: failedObservation,
			known:    activeFingerprint,
			hasKnown: true,
			desired:  "next-fp-2222",
			want:     "new_generation",
		},
		{
			name:     "serving generation without a failure reports the content change",
			cur:      activeGeneration,
			observed: succeededObservation,
			known:    activeFingerprint,
			hasKnown: true,
			desired:  "next-fp-2222",
			want:     "updated",
		},
		{
			name:     "unchanged content with a moved selection reports selection",
			cur:      activeGeneration,
			observed: succeededObservation,
			known:    activeFingerprint,
			hasKnown: true,
			desired:  activeFingerprint,
			want:     "selection",
		},
		{
			name:     "a failed row without a desired fingerprint falls through to the registry entry",
			cur:      activeGeneration,
			observed: failedWithoutFingerprint,
			known:    activeFingerprint,
			hasKnown: true,
			desired:  "next-fp-2222",
			want:     "updated",
		},
	}

	// when each preparation is classified
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := preparationOutcome(tc.cur, tc.observed, tc.known, tc.hasKnown, tc.desired)

			// then the logged outcome is the expected classification
			if got != tc.want {
				t.Errorf("preparationOutcome(...) = %q, want %q", got, tc.want)
			}
		})
	}
}
