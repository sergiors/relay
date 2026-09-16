package python

import (
	"fmt"
	"strings"
)

// ServiceCommand returns the container entrypoint that executes a validated
// service entrypoint FILE path as a Python module.
//
// A service entrypoint is the application startup FILE (a relative path inside
// the function directory, e.g. "app/main.py" or "main.py"). Python executes a
// service as a module (python -m <module>) rather than as a script
// (python <file>) so package-relative imports work: inside app/main.py a sibling
// import such as `from .deps import ...` only resolves when the interpreter runs
// the package as a module from the function directory (the image WORKDIR /app),
// not when it runs the file as a bare script. The node runtime has no such
// requirement and executes the file directly; that translation lives in the
// runtime package's ServiceEntry switch. This function is the ONLY place Python
// service execution is decided.
//
// The caller must already have applied the generic relative-path rules from the
// runtime package (non-empty, no whitespace/backslash, not absolute, and every
// "/"-separated element non-empty, not "."-leading, not ".."). ServiceCommand
// accepts that contract and enforces the Python-specific constraints on top:
//
//   - the path must end in ".py" (case-sensitive), so only importable Python
//     source files are launchable;
//   - every path element must be a valid Python identifier
//     ([A-Za-z_] then [A-Za-z0-9_]*), because each becomes a module/package
//     component after the "/" → "." conversion. A dash-filled filename like
//     "my-file.py" is rejected here: such a file cannot be imported as a module
//     anyway, and mangling it would silently misbehave.
//
// It returns ["python", "-m", <module-minus-.py>], e.g. "app/main.py" →
// ["python", "-m", "app.main"].
func ServiceCommand(entrypoint string) ([]string, error) {
	if !strings.HasSuffix(entrypoint, ".py") {
		return nil, fmt.Errorf("service entrypoint %q: python services require a .py module file", entrypoint)
	}
	moduleName := strings.TrimSuffix(entrypoint, ".py")
	for _, el := range strings.Split(moduleName, "/") {
		if !validIdentifier(el) {
			return nil, fmt.Errorf("service entrypoint %q: %q is not a valid Python module name", entrypoint, moduleName)
		}
	}
	return []string{"python", "-m", strings.ReplaceAll(moduleName, "/", ".")}, nil
}

// validIdentifier reports whether s is a conservative Python identifier:
// [A-Za-z_][A-Za-z0-9_]*. Relay intentionally stays ASCII-conservative even
// though Python permits Unicode identifiers, so a module name Relay constructs
// is always importable. An empty string (which the generic path validator
// already rejects upstream, but which is checked here for defense) is invalid.
func validIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') {
			continue
		}
		if i > 0 && '0' <= c && c <= '9' {
			continue
		}
		return false
	}
	return true
}
