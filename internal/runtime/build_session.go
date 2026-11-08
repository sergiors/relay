package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/moby/buildkit/session"
	"github.com/moby/moby/client"
)

// buildSessionHandle is the part of a live BuildKit session a build needs: the
// ID named on the ImageBuild request and the teardown that ends its transport.
type buildSessionHandle interface {
	ID() string
	Close() error
}

// buildSession is the live BuildKit session attached to one ImageBuild request.
//
// The Engine API's version=2 (BuildKit) build path resolves remote source
// metadata and registry credentials through the client session named by
// ImageBuildOptions.SessionID. BuildKit does not fall back to anonymous access
// when the group contains no session: older daemons surface "no active
// sessions" while resolving a base image, before the Dockerfile context is
// even consumed. Newer daemons tolerate that particular error, so the failure
// is version-dependent; attaching a real session makes the build independent of
// that difference.
type buildSession struct {
	session *session.Session
}

// connectBuildSession connects a BuildKit session to the daemon and returns
// once the session transport is established, so a following ImageBuild can name
// it. The daemon registers the session while it negotiates the hijacked
// connection; the image build's source resolver then waits for that registration
// by ID rather than failing with ErrNoActiveSessions. The session lives until
// Close; callers must always close it.
func connectBuildSession(ctx context.Context, cli *client.Client) (buildSessionHandle, error) {
	s, err := session.NewSession(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("create BuildKit session: %w", err)
	}

	connected := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(ctx context.Context, proto string, meta map[string][]string) (net.Conn, error) {
			conn, err := cli.DialHijack(ctx, "/session", proto, meta)
			if err != nil {
				return nil, err
			}
			// Buffered: the caller may already have given up on ctx.
			connected <- struct{}{}
			return conn, nil
		})
	}()

	select {
	case <-connected:
		return &buildSession{session: s}, nil
	case err := <-done:
		_ = s.Close()
		if err == nil {
			err = errors.New("session ended before connecting")
		}
		return nil, fmt.Errorf("connect BuildKit session: %w", err)
	case <-ctx.Done():
		_ = s.Close()
		return nil, ctx.Err()
	}
}

// ID is the session identifier passed to the daemon on the build request.
func (s *buildSession) ID() string {
	return s.session.ID()
}

// Close tears down the session and waits for its transport goroutine.
func (s *buildSession) Close() error {
	return s.session.Close()
}
