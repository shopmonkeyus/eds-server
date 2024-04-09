//go:build sign
// +build sign

package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/k0kubun/go-ansi"
	"github.com/shopmonkeyus/go-common/logger"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

var builds = [][]string{
	{"darwin", "arm64", "darwin-arm64"},
	{"darwin", "amd64", "darwin-amd64"},
	{"linux", "amd64", "linux-x64"},
}

func getGitSHA(ctx context.Context, logger logger.Logger) string {
	thesha := os.Getenv("GIT_SHA")
	if thesha != "" {
		return thesha
	}
	c := exec.CommandContext(ctx, "git", "rev-parse", "--abbrev-ref", "HEAD")
	buf, err := c.CombinedOutput()
	if err != nil {
		logger.Error("error running git: %s", err)
		os.Exit(1)
	}
	branch := strings.TrimSpace(string(buf))
	if branch == "main" {
		c := exec.CommandContext(ctx, "git", "rev-parse", "origin/main")
		buf, err := c.CombinedOutput()
		if err != nil {
			logger.Error("error running git rev-parse: %s", err)
			os.Exit(1)
		}
		return strings.TrimSpace(string(buf))
	}
	// inside a PR, we need to go back one
	c = exec.CommandContext(ctx, "git", "log", "--pretty=oneline", "-n", "2")
	buf, err = c.CombinedOutput()
	if err != nil {
		logger.Error("error running git rev-parse: %s", err)
		os.Exit(1)
	}
	lines := string(buf)
	tokens := strings.Split(lines, "\n")
	line := tokens[1]
	return strings.TrimSpace(strings.Split(line, " ")[0])
}

func executeBuild(ctx context.Context, logger logger.Logger, goos string, arch string, name string, dest string, sha string) {
	env := make([]string, len(os.Environ()))
	copy(env, os.Environ())
	env = append(env, "GOOS="+goos)
	env = append(env, "GOARCH="+arch)
	fn := path.Join(dest, name)
	c := exec.CommandContext(ctx, "go", "build", "-ldflags", "-s -w -X 'github.com/shopmonkeyus/eds-server/cmd.Version="+sha+"'", "-o", fn)
	c.Env = env
	c.Stdin = os.Stdin
	c.Stderr = os.Stderr
	c.Stdout = io.Discard
	if err := c.Run(); err != nil {
		logger.Error("error running go build: %s", err)
		os.Exit(1)
	}
	logger.Info("built: %s to %s", name, fn)
}

func executeLipo(ctx context.Context, logger logger.Logger, output string, a string, b string) {
	cwd, _ := os.Getwd()
	c := exec.CommandContext(ctx, "lipo", "-create", "-output", output, a, b)
	c.Dir = cwd
	c.Stdin = os.Stdin
	c.Stderr = os.Stderr
	c.Stdout = os.Stdout
	if err := c.Run(); err != nil {
		logger.Error("error running lipo: %s", err)
		os.Exit(1)
	}
	logger.Info("created lipo at %s", output)
}

type teeWriter struct {
	data strings.Builder
}

var _ io.Writer = (*teeWriter)(nil)

func (w *teeWriter) Write(p []byte) (int, error) {
	w.data.Write(p)
	return os.Stdout.Write(p)
}

func exists(fn string) bool {
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return false
	}
	return true
}

func executeGon(ctx context.Context, logger logger.Logger, outdir string) {
	cwd, _ := os.Getwd()
	fn := path.Join(cwd, "sign.hcl")
	if !exists(fn) {
		logger.Error("file not found: %s", fn)
		os.Exit(1)
	}
	if os.Getenv("CI") == "true" {
		buf, err := os.ReadFile(fn)
		if err != nil {
			logger.Error("error reading fn: %s: %s", fn, err)
			os.Exit(1)
		}
		s := strings.ReplaceAll(string(buf), `"./build/eds-server-darwin"`, `"`+path.Join(outdir, "eds-server-darwin")+`"`)
		s = strings.ReplaceAll(s, "eds-server-darwin.zip", path.Join(cwd, "eds-server-darwin.zip"))
		if err := os.WriteFile(fn, []byte(s), 0644); err != nil {
			logger.Error("error writing fn: %s: %s", fn, err)
			os.Exit(1)
		}
	}
	cwd, _ = filepath.Abs(cwd)
	os.Remove(path.Join(outdir, "eds-server-darwin-amd64"))
	os.Remove(path.Join(outdir, "eds-server-darwin-arm64"))
	started := time.Now()
	logger.Info("Apple notarization requested ...")

	var w teeWriter
	c := exec.CommandContext(ctx, "gon", fn)
	c.Dir = cwd
	c.Env = os.Environ()
	c.Stdin = os.Stdin
	c.Stderr = os.Stderr
	c.Stdout = &w
	if err := c.Run(); err != nil {
		logger.Error("error running gon: %s", err)
		os.Exit(1)
	}
	if !strings.Contains(w.data.String(), "Notarization complete!") {
		logger.Error("error performing notarization: %s", w.data.String())
		os.Exit(1)
	}
	os.Remove(path.Join(outdir, "eds-server-darwin"))
	os.Rename(path.Join(cwd, "eds-server-darwin.zip"), path.Join(outdir, "eds-server-darwin.zip"))

	logger.Info("Apple notarization completed in %v", time.Since(started))
}

var signCmd = &cobra.Command{
	Use:   "sign",
	Short: "Code-signs binaries which includes cross platform binaries",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logger.NewConsoleLogger()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sha := getGitSHA(ctx, logger)
		logger.Info("building binaries for sha: %s", sha)
		outdir, _ := cmd.Flags().GetString("dir")
		if outdir == "" {
			tmpdir := os.TempDir()
			outdir := path.Join(tmpdir, fmt.Sprintf("eds-server-build-%d", time.Now().Unix()))
			defer os.RemoveAll(outdir)
		}
		outdir, _ = filepath.Abs(outdir)
		os.MkdirAll(outdir, 0755)
		var wg sync.WaitGroup
		for _, build := range builds {
			wg.Add(1)
			goos := build[0]
			arch := build[1]
			suffix := build[2]
			name := fmt.Sprintf("eds-server-%s", suffix)
			go func() {
				defer wg.Done()
				executeBuild(ctx, logger, goos, arch, name, outdir, sha)
			}()
		}
		go func() {
			defer cancel()
			<-csys.CreateShutdownChannel()
			ansi.EraseInLine(3) // clear the CTRL+C break character
		}()
		wg.Wait()
		select {
		case <-ctx.Done():
			return
		default:
		}
		executeLipo(ctx, logger, path.Join(outdir, "eds-server-darwin"), path.Join(outdir, "eds-server-darwin-arm64"), path.Join(outdir, "eds-server-darwin-amd64"))
		executeGon(ctx, logger, outdir)

	},
}

func init() {
	rootCmd.AddCommand(signCmd)
	signCmd.Flags().String("dir", "./build", "the output directory")
}
