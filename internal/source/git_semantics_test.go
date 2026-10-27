//go:build integration

package source

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitIgnoreMatchesGitSemantics(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Fatalf("Git executable is required for these integration tests: %v", err)
	}

	patterns := []string{
		"*.log",
		"/build",
		"build/",
		"!keep",
		`\#hash`,
		`foo\ `,
		"foo bar",
		"a/**/b",
		"**/c",
		"[abc]x",
		" foo",
		`\!bang`,
		"#comment",
		"dir/**",
		"*.log\n!important.log",
		"foo   ",
		"!sub/a.log",
		"sub/",
		"*.log\n!sub/a.log",
		"/a.log",
		`\ `,
		"a\\b",
		"trail\\ ",
		"a/b/",
	}
	files := []string{
		"a.log", "keep", "#hash", "!bang", "foo ", "foo bar", "abx", "ax", "b.log",
		"important.log", "sub/a.log", "sub/keep", "build/x", "dir/y", "a/b", "a/c/b",
		"a\\b", "trail ",
	}
	dirs := []string{"sub", "build", "dir", "a", "a/c", "**"}

	for _, pat := range patterns {
		repo := t.TempDir()
		// write layout
		for _, d := range dirs {
			os.MkdirAll(filepath.Join(repo, d), 0o755)
		}
		for _, f := range files {
			os.MkdirAll(filepath.Dir(filepath.Join(repo, f)), 0o755)
			os.WriteFile(filepath.Join(repo, f), []byte("x"), 0o644)
		}
		os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(pat+"\n"), 0o644)

		// git
		if err := exec.Command("git", "-C", repo, "init", "-q").Run(); err != nil {
			t.Fatalf("git init in %q: %v", repo, err)
		}
		var gitExcluded []string
		for _, f := range files {
			if gitCheckIgnored(t, repo, f) {
				gitExcluded = append(gitExcluded, f)
			}
		}
		// our source
		selection, err := ForDir(repo)
		if err != nil {
			t.Fatalf("ForDir: %v", err)
		}
		var ourExcluded []string
		for _, f := range files {
			if !selection.Includes(f, false) {
				ourExcluded = append(ourExcluded, f)
			}
		}
		// also dirs
		for _, d := range dirs {
			g := gitCheckIgnored(t, repo, d)
			o := !selection.Includes(d, true)
			if g != o {
				t.Errorf("PATTERN %q DIR %q: git=%v ours=%v", pat, d, g, o)
			}
		}
		if strings.Join(gitExcluded, ",") != strings.Join(ourExcluded, ",") {
			t.Errorf("PATTERN %q:\n git  = %v\n ours = %v", pat, gitExcluded, ourExcluded)
		}
	}
}

func gitCheckIgnored(t *testing.T, repo, path string) bool {
	t.Helper()
	cmd := exec.Command("git", "-C", repo, "check-ignore", "-q", "--", path)
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false
	}
	t.Fatalf("git check-ignore %q in %q: %v", path, repo, err)
	return false
}
