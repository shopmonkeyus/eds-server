package provider

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
)

var EOL = []byte("\n")

type FileProvider struct {
	logger           logger.Logger
	cmd              *exec.Cmd
	stdin            io.WriteCloser
	stdout           io.ReadCloser
	scanner          *bufio.Scanner
	verbose          bool
	once             sync.Once
	schemaModelCache *map[string]dm.Model
}

var _ internal.Provider = (*FileProvider)(nil)

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewFileProvider(plogger logger.Logger, cmd []string, schemaModelCache *map[string]dm.Model, opts *ProviderOpts) (internal.Provider, error) {
	logger := plogger.WithPrefix(fmt.Sprintf("[file] [%s]", cmd[0]))
	logger.Info("file provider will execute program: %s", cmd[0])
	if _, err := os.Stat(cmd[0]); os.IsNotExist(err) {
		return nil, fmt.Errorf("couldn't find: %s", cmd[0])
	}
	theCmd := exec.Command(cmd[0], cmd[1:]...)
	theCmd.Stderr = os.Stderr
	stdin, err := theCmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin: %w", err)
	}
	stdout, err := theCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout: %w", err)
	}
	scanner := bufio.NewScanner(stdout)
	return &FileProvider{
		logger:           logger,
		cmd:              theCmd,
		stdin:            stdin,
		stdout:           stdout,
		scanner:          scanner,
		verbose:          opts.Verbose,
		schemaModelCache: schemaModelCache,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *FileProvider) Start() error {
	p.logger.Info("start")
	if err := p.cmd.Start(); err != nil {
		return err
	}
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *FileProvider) Stop() error {
	p.logger.Info("stop")
	p.once.Do(func() {
		p.logger.Debug("sending kill signal to pid: %d", p.cmd.Process.Pid)
		if p.cmd.Process != nil {
			toKill, _ := process.NewProcess(int32(p.cmd.Process.Pid))
			toKill.SendSignal(syscall.SIGINT)
			p.stdin.Close()
		}
		p.logger.Debug("stopped")
	})
	return nil
}

func (p *FileProvider) readStout() error {
	for {
		for p.scanner.Scan() {
			line := p.scanner.Text()
			if p.verbose {
				p.logger.Debug("incoming stdout read: <%s>", line)
			}
			if strings.Contains(line, "OK") {
				p.logger.Debug("success processing message: <%s>", line)
				return nil
			} else if strings.Contains(line, "ERR") {
				return fmt.Errorf("error processing message: <%s>", line)
			} else {
				if p.verbose {
					p.logger.Debug("stdout read: <%s>", line)
				}
			}

		}
		if err := p.scanner.Err(); err != nil {
			p.logger.Error("error reading stdout:", err)
			return nil
		} else {
			p.logger.Warn("got EOF, restarting scan")
		}
	}
}

// Process data received and return an error or nil if processed ok
func (p *FileProvider) Process(data datatypes.ChangeEventPayload, schema dm.Model) error {
	transport := datatypes.Transport{
		DBChange: data,
		Schema:   schema,
	}
	buf, err := json.Marshal(transport)
	if err != nil {
		return fmt.Errorf("error converting to json: %s", err)
	}
	if _, err := p.stdin.Write(buf); err != nil {
		return fmt.Errorf("stdin: %w", err)
	}
	if _, err := p.stdin.Write(EOL); err != nil {
		return fmt.Errorf("stdin: %w", err)
	}

	err = p.readStout()

	if err != nil {
		p.logger.Error("error in file provider, exiting %s", err.Error())
		return err
	}
	p.logger.Debug("exiting file provider")
	return nil
}

// TODO: Implement this method
func (p *FileProvider) Import(dataMap map[string]interface{}, tableName string, nc *nats.Conn) error {
	return nil
}
