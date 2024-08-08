package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"

	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/spf13/cobra"
)

type enrollTokenData struct {
	Token    string `json:"token" toml:"server_id`
	ServerID string `json:"serverId" toml:"server_id"`
}

type enrollResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    enrollTokenData `json:"data"`
}

func getAPIURL(firstLetter string) (*string, error) {
	apiUrls := map[string]string{
		"P": "https://api.shopmonkey.cloud",
		"S": "https://sandbox-api.shopmonkey.cloud",
		"E": "https://edge-api.shopmonkey.cloud",
		"L": "http://localhost:3101",
	}

	if url, exists := apiUrls[firstLetter]; exists {
		return &url, nil
	}
	return nil, errors.New("invalid code")
}

var enrollCmd = &cobra.Command{
	Use:   "enroll [code]",
	Short: "Enroll a new server and get api key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[enroll]")
		code := args[0]
		apiURL := mustFlagString(cmd, "api-url", false)
		dataDir := getDataDir(cmd, logger)

		if apiURL == "" {
			logger.Debug("Getting api from prefix")
			firstLetter := code[0:1]
			maybeApiURL, err := getAPIURL(firstLetter)
			if err != nil {
				logger.Fatal("error getting api url: %s", err)
			}
			apiURL = *maybeApiURL
		}

		req, err := http.NewRequest("GET", apiURL+"/v3/eds/internal/enroll/"+code, nil)
		if err != nil {
			logger.Fatal("error creating request: %s", err)
		}

		retry := util.NewHTTPRetry(req)
		resp, err := retry.Do()
		if err != nil {
			logger.Fatal("failed to enroll server: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			logger.Fatal("%s", handleAPIError(resp, "enroll"))
		}

		var enrollResp enrollResponse
		if err := json.NewDecoder(resp.Body).Decode(&enrollResp); err != nil {
			logger.Fatal("failed to decode response: %w", err)
		}
		if !enrollResp.Success {
			logger.Fatal("failed to start enroll: %s", enrollResp.Message)
		}

		var buf bytes.Buffer
		if err := toml.NewEncoder(&buf).Encode(enrollResp.Data); err != nil {
			log.Fatal(err)
		}

		tokenFile := filepath.Join(dataDir, "config.toml")
		file, err := os.Create(tokenFile)
		if err != nil {
			logger.Fatal("failed to create token file: %w", err)
		}
		if err := os.WriteFile(tokenFile, buf.Bytes(), 0644); err != nil {
			log.Fatal(err)
		}
		if err != nil {
			logger.Fatal("failed to write to token file: %w", err)
		}
		file.Close()
	},
}

func init() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("couldn't get current working directory: ", err)
		os.Exit(1)
	}
	rootCmd.AddCommand(enrollCmd)
	enrollCmd.Flags().String("api-url", "", "the for testing again preview environment")
	enrollCmd.Flags().MarkHidden("api-url")
	enrollCmd.Flags().String("data-dir", filepath.Join(cwd, "datadir"), "the data directory for storing logs and other data")
}
