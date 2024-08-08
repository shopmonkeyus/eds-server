package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/notification"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/command"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

var Version string                // set in main
var ShopmonkeyPublicPGPKey string // set in main

const maxFailures = 5

type driverMeta struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	URL         string `json:"url"` // this is masked since it can contain sensitive information
}

type sessionStart struct {
	Version   string     `json:"version"`
	Hostname  string     `json:"hostname"`
	IPAddress string     `json:"ipAddress"`
	MachineId string     `json:"machineId"`
	OsInfo    any        `json:"osinfo"`
	Driver    driverMeta `json:"driver"`
}

type edsSession struct {
	SessionId  string  `json:"sessionId"`
	Credential *string `json:"credential"`
}

type sessionStartResponse struct {
	Success bool       `json:"success"`
	Message string     `json:"message"`
	Data    edsSession `json:"data"`
}

type sessionEnd struct {
	Errored bool `json:"errored"`
}

type sessionEndURLs struct {
	URL      string `json:"url"`
	ErrorURL string `json:"errorUrl"`
}

type sessionEndResponse struct {
	Success bool           `json:"success"`
	Message string         `json:"message"`
	Data    sessionEndURLs `json:"data"`
}

type sessionRenewResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    struct {
		Credential *string `json:"credential"`
	} `json:"data"`
}

func writeCredsToFile(data string, filename string) error {
	buf, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return fmt.Errorf("failed to decode base64: %w", err)
	}
	if err := os.WriteFile(filename, buf, 0600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

func sendStart(logger logger.Logger, apiURL string, apiKey string, driverUrl string) (*edsSession, error) {
	var body sessionStart
	ipaddress, err := util.GetLocalIP()
	if err != nil {
		return nil, fmt.Errorf("failed to get local IP: %w", err)
	}
	machineid, err := util.GetMachineId()
	if err != nil {
		return nil, fmt.Errorf("failed to get machine ID: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	osinfo, err := util.GetSystemInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get system info: %w", err)
	}
	body.MachineId = machineid
	body.IPAddress = ipaddress
	body.Hostname = hostname
	body.Version = Version
	body.OsInfo = osinfo

	driverMeta, err := internal.GetDriverMetadataForURL(driverUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to get driver metadata: %w", err)
	}
	if driverMeta == nil {
		return nil, fmt.Errorf("invalid driver URL: %s", driverUrl)
	}
	body.Driver.Description = driverMeta.Description
	body.Driver.Name = driverMeta.Name
	body.Driver.ID = driverMeta.Scheme
	body.Driver.URL, err = util.MaskURL(driverUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to mask driver URL: %w", err)
	}

	logger.Trace("sending session start: %s", util.JSONStringify(body))

	req, err := http.NewRequest("POST", apiURL+"/v3/eds/internal", bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send session start: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, handleAPIError(resp, "session start")
	}
	var sessionResp sessionStartResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessionResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !sessionResp.Success {
		return nil, fmt.Errorf("failed to start session: %s", sessionResp.Message)
	}
	logger.Trace("session %s started successfully", sessionResp.Data.SessionId)
	return &sessionResp.Data, nil
}

func sendEnd(logger logger.Logger, apiURL string, apiKey string, sessionId string, errored bool) (*sessionEndURLs, error) {
	var body sessionEnd
	body.Errored = errored
	req, err := http.NewRequest("POST", apiURL+"/v3/eds/internal/"+sessionId, bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send session end: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, handleAPIError(resp, "session end")
	}
	var s sessionEndResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return nil, fmt.Errorf("failed to end session: %s", s.Message)
	}
	logger.Trace("session %s ended successfully: %s", sessionId, s.Data.URL)
	return &s.Data, nil
}

func sendRenew(logger logger.Logger, apiURL string, apiKey string, sessionId string) (*string, error) {
	req, err := http.NewRequest("POST", apiURL+"/v3/eds/internal/renew/"+sessionId, strings.NewReader("{}"))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send renew end: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, handleAPIError(resp, "session renew")
	}
	var s sessionRenewResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return nil, fmt.Errorf("failed to renew session: %s", s.Message)
	}
	logger.Trace("session %s renew successfully", sessionId)
	return s.Data.Credential, nil
}

func uploadFile(logger logger.Logger, url string, logFileBundle string) error {
	of, err := os.Open(logFileBundle)
	if err != nil {
		return fmt.Errorf("failed to open log file bundle: %w", err)
	}
	defer of.Close()
	req, err := http.NewRequest("PUT", url, of)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, "")
	req.Header.Set("Content-Type", "application/x-tgz")
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return fmt.Errorf("failed to upload logs: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return handleAPIError(resp, "upload logs")
	}
	logger.Trace("logs uploaded successfully: %s", logFileBundle)
	return nil
}

func uploadLogFile(logger logger.Logger, uploadURL string, logFile string) (string, error) {
	logger.Debug("uploading logfile: %s", logFile)
	// gzip the log file, do this first in case we get an error!
	if err := util.GzipFile(logFile); err != nil {
		return "", fmt.Errorf("failed to compress log file: %w", err)
	}
	compressedLogFile := logFile + ".gz"
	defer os.Remove(compressedLogFile)

	if err := uploadFile(logger, uploadURL, compressedLogFile); err != nil {
		return "", fmt.Errorf("failed to upload logs to %s: %s", uploadURL, err)
	}
	parsedURL, err := url.Parse(uploadURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse upload URL: %w", err)
	}
	return parsedURL.Path, nil
}

func getRemainingLog(logsDir string) (string, error) {
	files, err := util.ListDir(logsDir)
	if err != nil {
		return "", fmt.Errorf("failed to list directory: %w", err)
	}
	if len(files) == 0 {
		return "", nil
	}
	sort.Strings(files)

	var last string
	for _, file := range files {
		if !strings.HasSuffix(file, ".log") {
			continue
		}
		last = file
	}
	return last, nil
}

func sendEndAndUpload(logger logger.Logger, apiurl string, apikey string, sessionId string, errored bool, logfile string, stderrFile string) (string, error) {
	logger.Info("uploading logs for session: %s", sessionId)
	urls, err := sendEnd(logger, apiurl, apikey, sessionId, errored)
	if err != nil {
		return "", fmt.Errorf("failed to send session end: %w", err)
	}
	var logFileStoragePath string
	if logfile != "" {
		logFileStoragePath, err = uploadLogFile(logger, urls.URL, logfile)
		if err != nil {
			return "", fmt.Errorf("failed to upload logs: %w", err)
		}
	}
	if urls.ErrorURL != "" && stderrFile != "" {
		if _, err := uploadLogFile(logger, urls.ErrorURL, stderrFile); err != nil {
			return "", fmt.Errorf("failed to upload error logs: %w", err)
		}
	}
	logger.Trace("logs uploaded successfully for session: %s", sessionId)
	if errored {
		logger.Info("error log files saved to %s for session: %s", logfile, sessionId)
	}
	return logFileStoragePath, nil
}

func getLogUploadURL(logger logger.Logger, apiURL string, apiKey string, sessionId string) (string, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/eds/internal/%s/log", apiURL, sessionId), strings.NewReader("{}"))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return "", fmt.Errorf("failed to get log upload url: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", handleAPIError(resp, "log upload url")
	}
	var s sessionEndResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return "", fmt.Errorf("failed to get log upload url: %s", s.Message)
	}
	logger.Trace("session %s log url received: %s", sessionId, s.Data.URL)
	return s.Data.URL, nil
}

var serverIgnoreFlags = map[string]bool{
	"--api-url":        true,
	"--api-key":        true,
	"--silent":         true,
	"--port":           true,
	"--health-port":    true,
	"--renew-interval": true,
	"--wrapper":        true,
	"--parent":         true,
}

func collectCommandArgs() []string {
	var skipping bool
	var _args []string
	for _, arg := range os.Args[2:] {
		if skipping {
			skipping = false
			continue
		}
		tok := strings.Split(arg, "=")
		_arg := tok[0]
		if serverIgnoreFlags[_arg] {
			skipping = true
			continue
		}
		_args = append(_args, arg)
	}
	return _args
}

// runWrapperLoop will run the main parent process which will fork the child process (server)
// which acts as a control mechanism for the fork process (which has all the real logic).
// this loop is only responsible for waiting for a restart signal and then restarting the child process.
func runWrapperLoop(logger logger.Logger) {
	logger = logger.WithPrefix("[wrapper]")
	logger.Trace("running the wrapper loop")
	port, err := util.GetFreePort()
	if err != nil {
		logger.Fatal("failed to get free port: %s", err)
	}

	parentProcess := os.Args[0] // start of with the original process

	logger.Trace("parent process port is: %d", port)

	exitCode := -1
	restart := make(chan bool, 1)
	exited := make(chan bool, 1)
	var waitGroup sync.WaitGroup
	var inUpgrade bool

	httphandler := &http.ServeMux{}
	httpsrv := &http.Server{
		Addr:           fmt.Sprintf("127.0.0.1:%d", port), // only bind to localhost so we don't expose externally
		Handler:        httphandler,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   1 * time.Second,
		IdleTimeout:    5 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}
	httphandler.HandleFunc("/restart", func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("received restart request")
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("failed to read body: %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.Body.Close()
		inUpgrade = true
		tok := strings.Split(string(buf), ",") // format is: version,command
		newVersion := tok[0]
		newCmd := tok[1]
		logger.Trace("received restart cmd: %s with expected version: %s", newCmd, newVersion)
		// NOTE: we do this is a separate goroutine so we can respond to the request w/o blocking
		// since we will need to shutdown the child process
		go func() {
			defer util.RecoverPanic(logger)
			var output bytes.Buffer
			cmd := exec.Command(newCmd, "version")
			cmd.Stdout = &output
			if err := cmd.Run(); err != nil {
				logger.Error("failed to run new command: %s", err)
			} else {
				if newVersion != strings.TrimSpace(output.String()) {
					logger.Error("new version does not match: %s != %s, not upgrading", newVersion, output.String())
				} else {
					logger.Debug("new version matches: %s", newVersion)
					logger.Debug("swapping out old process: %s with new process: %s", parentProcess, newCmd)
					parentProcess = newCmd
				}
			}
			// TODO: in all failure cases we need to send back some kind of error response to our API
			restart <- true
		}()
		w.WriteHeader(http.StatusAccepted)
	})

	go func() {
		defer util.RecoverPanic(logger)
		if err := httpsrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("failed to start wrapper server: %s", err)
		}
	}()

	args := os.Args[1:]
	args = append(args, "--wrapper")
	args = append(args, fmt.Sprintf("--parent=%d", port))

	shutdownHTTP := func() {
		defer util.RecoverPanic(logger)
		logger.Info("initiating HTTP server shutdown")
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelShutdown()
		if err := httpsrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP server shutdown error: %s", err)
		}
		logger.Info("HTTP server successfully shutdown")
	}

	var failures int
	var completed bool

	for failures < maxFailures && !completed {
		logger.Trace("starting process: %s %s", parentProcess, strings.Join(args, " "))
		cmd := exec.Command(parentProcess, args...)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmd.Stdin = os.Stdin
		if err := cmd.Start(); err != nil {
			logger.Fatal("failed to start: %s", err)
		}
		waitGroup.Add(1)
		go func() {
			defer util.RecoverPanic(logger)
			defer waitGroup.Done()
			if err := cmd.Wait(); err != nil {
				logger.Error("wrapper process exited: %s", err)
			}
			exitCode = cmd.ProcessState.ExitCode()
			logger.Trace("wrapper process exited with exit code: %d", exitCode)
			if !completed {
				exited <- true
			}
		}()
		select {
		case <-restart:
			logger.Trace("restarting process")
			cmd.Process.Signal(syscall.SIGINT)
		case <-sys.CreateShutdownChannel():
			logger.Trace("SIGINT received")
			cmd.Process.Signal(syscall.SIGINT)
			exitCode = 0
			completed = true
		case <-exited:
			logger.Trace("exit received: %d", exitCode)
			if inUpgrade && exitCode != 0 {
				inUpgrade = false
				failures++
				parentProcess = os.Args[0] // reset to the original process since the upgrade failed
				logger.Trace("upgrade failed, resetting to original process and restarting")
				continue
			}
			if exitCode == 0 || exitCode == 1 {
				completed = true
			} else {
				failures++
				time.Sleep(time.Second * time.Duration(failures))
			}
		}
	}

	logger.Trace("waiting for child processes to exit")
	waitGroup.Wait()
	logger.Trace("child process exited")
	shutdownHTTP()
	logger.Trace("exit with exit code: %d", exitCode)
	os.Exit(exitCode)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)

		logger = logger.WithPrefix("[server]")

		defer util.RecoverPanic(logger)

		wrapper := mustFlagBool(cmd, "wrapper", false)
		if !wrapper {
			runWrapperLoop(logger)
			return
		}

		apiurl := mustFlagString(cmd, "api-url", true)
		apikey := mustFlagString(cmd, "api-key", true)
		driverURL := mustFlagString(cmd, "url", true)
		server := mustFlagString(cmd, "server", true)
		dataDir := getDataDir(cmd, logger)

		apiurl = strings.TrimSuffix(apiurl, "/") // remove trailing slash

		if cmd.Flags().Changed("api-url") {
			logger.Info("using alternative API url: %s", apiurl)
		} else {
			url, err := util.GetAPIURLFromJWT(apikey)
			if err != nil {
				logger.Fatal("invalid API key. %s", err)
			}
			apiurl = url
		}

		var credsFile string
		var sessionDir string

		// must be in a defer to make sure we pick up credsFile variable
		defer func() {
			// make sure we remove the temporary credential
			os.Remove(credsFile)
			// if this is the last file in the directory, go ahead and remove it too
			if files, _ := os.ReadDir(sessionDir); len(files) == 0 {
				os.RemoveAll(sessionDir)
			}
		}()

		port := mustFlagInt(cmd, "port", true)
		oldHealthPort := mustFlagInt(cmd, "health-port", false)
		if oldHealthPort > 0 {
			port = oldHealthPort // allow it for now for backwards compatibility but eventually remove it
		}
		parentPort := mustFlagInt(cmd, "parent", true)

		_args := collectCommandArgs()
		_args = append(_args, "--port", fmt.Sprintf("%d", port))
		_args = append(_args, "--data-dir", dataDir)
		_args = append(_args, "--server", server)
		_args = append(_args, "--api-url", apiurl)

		var sessionId string

		processCallback := func(p *os.Process) {
			logger.Debug("fork process started with pid: %d", p.Pid)
		}

		restart := func() {
			logger.Info("need to restart")
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/restart", port))
			if err != nil {
				logger.Error("restart failed: %s", err)
			} else {
				logger.Debug("restart response: %d", resp.StatusCode)
			}
		}

		shutdown := func(msg string) {
			logger.Info("shutdown requested: %s", msg)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/shutdown", port))
			if err != nil {
				logger.Fatal("shutdown failed: %s", err)
			} else {
				logger.Debug("shutdown response: %d", resp.StatusCode)
			}
		}

		renew := func() {
			creds, err := sendRenew(logger, apiurl, apikey, sessionId)
			if err != nil {
				logger.Fatal("failed to renew session: %s", err)
			}
			if creds != nil {
				if err := writeCredsToFile(*creds, credsFile); err != nil {
					logger.Fatal("failed to write creds to file: %s", err)
				}
				restart()
			} else {
				logger.Trace("no new credentials to renew")
			}
		}

		pause := func() {
			logger.Info("server pause requested")
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/pause", port))
			if err != nil {
				logger.Error("pause failed: %s", err)
			} else {
				logger.Debug("pause response: %d", resp.StatusCode)
			}
		}

		unpause := func() {
			logger.Info("server unpause requested")
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/unpause", port))
			if err != nil {
				logger.Error("unpause failed: %s", err)
			} else {
				logger.Debug("unpause response: %d", resp.StatusCode)
			}
		}

		upgrade := func(version string, url string) {
			logger.Info("server upgrade requested to version: %s from url: %s", version, url)
			pause()
			// TODO - run the upgrade and when completed and ready, exit with special exit code for force a restart
			// to the new binary
			var body bytes.Buffer
			body.WriteString(version)
			body.WriteString(",")
			body.WriteString(os.Args[0]) /// FIXME: change this to the new binary
			resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/restart", parentPort), "text/plain", &body)
			if err != nil {
				logger.Error("restart failed: %s", err)
			} else {
				logger.Debug("restart response: %d", resp.StatusCode)
			}
		}

		var logsLock sync.Mutex
		sendLogs := func() *notification.SendLogsResponse {
			logger.Info("server logfile requested")
			logsLock.Lock()
			defer logsLock.Unlock()
			if sessionId == "" {
				logger.Error("no session ID to rotate logs")
				return nil
			}
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/logfile", port))
			if err != nil {
				logger.Error("logfile failed: %s", err)
				return nil
			}
			logger.Debug("logfile response: %d", resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				logger.Error("logfile failed: %d", resp.StatusCode)
				return nil
			}
			defer resp.Body.Close()

			buf, err := io.ReadAll(resp.Body)
			if err != nil {
				logger.Error("failed to read body: %s", err)
				return nil
			}
			logFile := string(buf)
			uploadURL, err := getLogUploadURL(logger, apiurl, apikey, sessionId)
			if err != nil {
				logger.Error("failed to get upload URL: %s", err)
				return nil
			}
			path, err := uploadLogFile(logger, uploadURL, logFile)
			if err != nil {
				logger.Error("failed to upload logfile: %s", err)
				return nil
			}

			// fork will be done writing to the file, so we can remove it
			logger.Debug("removing old logfile: %s", logFile)
			os.Remove(logFile)

			return &notification.SendLogsResponse{
				Path:      path,
				SessionId: sessionId,
			}
		}

		natsurl := mustFlagString(cmd, "server", true)

		// create a notification consumer that will listen for notification actions and handle them here
		notificationConsumer := notification.New(logger, natsurl, notification.NotificationHandler{
			Restart:  restart,
			Renew:    renew,
			Shutdown: shutdown,
			Pause:    pause,
			Unpause:  unpause,
			Upgrade:  upgrade,
			SendLogs: sendLogs,
		})

		// setup tickers
		duration, _ := cmd.Flags().GetDuration("renew-interval")
		logger.Trace("will renew session every %v", duration)
		renewTicker := time.NewTicker(duration)
		logSenderTicker := time.NewTicker(time.Hour)
		defer renewTicker.Stop()
		defer logSenderTicker.Stop()
		go func() {
			defer util.RecoverPanic(logger)
			for {
				select {
				case <-logSenderTicker.C:
					// ask the notification consumer to send the logs so it can report the success/failure
					notificationConsumer.CallSendLogs()
				case <-renewTicker.C:
					renew()
				}
			}
		}()

		// main loop
		var failures int
		for {
			if failures >= maxFailures {
				logger.Fatal("too many failures after %d attempts, exiting", failures)
			}
			session, err := sendStart(logger, apiurl, apikey, driverURL)
			if err != nil {
				logger.Fatal("failed to send session start: %s", err)
			}
			logger.Trace("session started: %s", util.JSONStringify(session))
			sessionId = session.SessionId
			sessionDir = filepath.Join(dataDir, sessionId)
			if err := os.MkdirAll(sessionDir, 0700); err != nil {
				logger.Fatal("failed to create session directory: %s", err)
			}
			if credsFile == "" && session.Credential != nil {
				// write credential to file
				credsFile = filepath.Join(sessionDir, "nats.creds")
				if err := writeCredsToFile(*session.Credential, credsFile); err != nil {
					logger.Fatal("failed to write creds to file: %s", err)
				}
				logger.Trace("creds written to %s", credsFile)
			} else {
				logger.Trace("using creds file: %s", credsFile)
			}
			if err := notificationConsumer.Start(sessionId, credsFile); err != nil {
				logger.Fatal("failed to start notification consumer: %s", err)
			}
			sessionLogsDir := filepath.Join(sessionDir, "logs")
			args := append(_args,
				"--creds", credsFile,
				"--logs-dir", sessionLogsDir,
			)
			result, err := command.Fork(command.ForkArgs{
				Log:              logger,
				Command:          "fork",
				Args:             args,
				LogFilenameLabel: "server",
				SaveLogs:         true,
				LogFileSink:      true,
				WriteToStd:       true,
				ForwardInterrupt: true,
				ProcessCallback:  processCallback,
				Dir:              sessionDir,
			})
			if err != nil && result == nil {
				logger.Error("failed to fork: %s", err)
				failures++
			} else {
				ec := result.ProcessState.ExitCode()
				if ec != 3 {
					logFile, err := getRemainingLog(sessionLogsDir)
					if err != nil {
						logger.Error("failed to get remaining log: %s", err)
					}
					logsLock.Lock()
					logPath, err := sendEndAndUpload(logger, apiurl, apikey, session.SessionId, ec != 0, logFile, filepath.Join(sessionDir, "server_stderr.txt"))
					logsLock.Unlock()
					if err != nil {
						logger.Error("failed to send end and upload logs: %s", err)
					} else {
						if err := notificationConsumer.PublishSendLogsResponse(&notification.SendLogsResponse{Path: logPath, SessionId: sessionId}); err != nil {
							logger.Error("failed to publish send logs response: %s", err)
						}
					}
				}
				if ec == 0 {
					// on success, remove the logs
					os.RemoveAll(sessionDir)
					break
				}
				// if a "normal" exit code, just exit and remove the logs
				if ec == 3 || ec == 1 && (strings.Contains(result.LastErrorLines, "error: required flag") || strings.Contains(result.LastErrorLines, "Global Flags")) {
					notificationConsumer.Stop()
					os.Exit(ec)
				}
				failures++
				logger.Error("server exited with code %d", ec)
			}
			notificationConsumer.Stop()
		}
	},
}

var serverHelpCmd = &cobra.Command{
	Use:   "help [driver]",
	Short: "Get help for the server operations",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		yellow := color.New(color.FgYellow, color.Bold).SprintFunc()
		cyan := color.New(color.FgCyan).SprintFunc()
		black := color.New(color.FgBlack).SprintFunc()
		whiteBold := color.New(color.FgWhite, color.Bold).SprintFunc()
		white := color.New(color.FgWhite).SprintFunc()
		green := color.New(color.FgGreen).SprintFunc()
		blue := color.New(color.FgBlue, color.Bold).SprintFunc()
		driverMetadata := internal.GetDriverMetadata()
		fmt.Println()
		fmt.Printf("%s\n", blue("Shopmonkey Enterprise Data Streaming (EDS) Server"))
		fmt.Printf("%s\n", black("version: "+Version))
		fmt.Println()
		if len(args) == 0 {
			fmt.Println("This server version supports the following integrations:")
			fmt.Println()
			for _, metadata := range driverMetadata {
				fmt.Printf("%-25s%s\n", yellow(metadata.Name), whiteBold(metadata.Description))
				fmt.Printf("%14s%s: %s\n", "", black("Example url"), cyan(metadata.ExampleURL))
				fmt.Println()
			}
			fmt.Println()
			fmt.Println("Example usage:")
			fmt.Println()
			c := filepath.Base(os.Args[0])
			if Version == "dev" {
				c = "go run . "
			}
			fmt.Printf(" $ %s\n", green(c+" server --url "+driverMetadata[0].ExampleURL+" --api-key $TOKEN --creds /path/to/creds.json"))
			fmt.Println()
			fmt.Println(black("To get a full list of options, pass in the --help flag."))
			fmt.Println()
			fmt.Println(black("To get more detailed help for a specific integration run: " + c + "server help [name]"))
			fmt.Println()
		} else {
			var metadata *internal.DriverMetadata
			for _, driver := range driverMetadata {
				if driver.Name == args[0] {
					metadata = &driver
					break
				}
			}
			if metadata == nil {
				fmt.Printf("No integration named %s is not found.\n", yellow(args[0]))
				fmt.Println()
				os.Exit(1)
			}
			fmt.Printf("%s\n", yellow(metadata.Name))
			fmt.Printf("%s\n", white(metadata.Description))
			fmt.Printf("%s: %s\n", black("Example url"), cyan(metadata.ExampleURL))
			fmt.Println()
			fmt.Println(metadata.Help)
			fmt.Println()
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverHelpCmd)

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("couldn't get current working directory: ", err)
		os.Exit(1)
	}

	// NOTE: sync these with forkCmd
	serverCmd.Flags().String("url", "", "driver connection string")
	serverCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey API key")
	serverCmd.Flags().Int("port", getOSInt("PORT", 8080), "the port to listen for health checks, metrics etc")
	serverCmd.Flags().String("data-dir", cwd, "the data directory for storing logs and other data")

	// deprecated but left for backwards compatibility
	serverCmd.Flags().Int("health-port", 0, "the port to listen for health checks")
	serverCmd.Flags().MarkDeprecated("health-port", "use --port instead")

	// internal use only
	serverCmd.Flags().String("schema", "schema.json", "the shopmonkey schema file")
	serverCmd.Flags().MarkHidden("schema")
	serverCmd.Flags().String("tables", "tables.json", "the shopmonkey tables file")
	serverCmd.Flags().MarkHidden("tables")
	serverCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	serverCmd.Flags().MarkHidden("api-url")
	serverCmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	serverCmd.Flags().MarkHidden("server")
	serverCmd.Flags().String("consumer-suffix", "", "suffix which is appended to the nats consumer group name")
	serverCmd.Flags().MarkHidden("consumer-suffix")
	serverCmd.Flags().Int("maxAckPending", defaultMaxAckPending, "the number of max ack pending messages")
	serverCmd.Flags().MarkHidden("maxAckPending")
	serverCmd.Flags().Int("maxPendingBuffer", defaultMaxPendingBuffer, "the maximum number of messages to pull from nats to buffer")
	serverCmd.Flags().MarkHidden("maxPendingBuffer")
	serverCmd.Flags().Bool("restart", false, "restart the consumer from the beginning (only works on new consumers)")
	serverCmd.Flags().MarkHidden("restart")
	serverCmd.Flags().Duration("renew-interval", time.Hour*24, "the interval to renew the session")
	serverCmd.Flags().MarkHidden("renew-interval")
	serverCmd.Flags().Bool("wrapper", false, "running in wrapper mode")
	serverCmd.Flags().MarkHidden("wrapper")
	serverCmd.Flags().Int("parent", -1, "the parent pid")
	serverCmd.Flags().MarkHidden("parent")
}
