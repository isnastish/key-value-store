package testsetup

import (
	"context"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/isnastish/kvs/pkg/log"
)

func startDockerContainer(expectedOutput, command string, args ...string) (bool, error) {
	tearDown := false
	cmd := exec.Command(command, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return tearDown, err
	}
	defer stdout.Close()

	if err := cmd.Start(); err != nil {
		return tearDown, err
	}

	tearDown = true // tearDown the container running the kvs-service

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var strBuilder strings.Builder

	readBuf := make([]byte, 256)
	for {
		select {
		case <-ctx.Done():
			return tearDown, ctx.Err()
		default:
		}

		n, err := stdout.Read(readBuf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Logger.Fatal("Reached end of stdout %v", err)
		}
		if n > 0 {
			outStr := string(readBuf[:n])
			log.Logger.Info(outStr)
			strBuilder.WriteString(outStr)
			if strings.Contains(strBuilder.String(), expectedOutput) {
				time.Sleep(500 * time.Millisecond)
				break
			}
		}
	}
	<-time.After(200 * time.Millisecond)
	return tearDown, nil
}

func killDockerContainer(command string, args ...string) {
	log.Logger.Info("Killing docker container")
	cmd := exec.Command(command, args...)
	if err := cmd.Run(); err != nil {
		log.Logger.Error("Failed to kill container")
		return
	}
	log.Logger.Info("Successfully killed the container")
}
