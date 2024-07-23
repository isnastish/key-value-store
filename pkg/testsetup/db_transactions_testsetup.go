package testsetup

import (
	"fmt"
)

var postgresDockerImage = "postgres:16.3"

func StartPostgresContainer(hostPort int, password string) (bool, error) {
	expectedOutput := "PostgreSQL init process complete; ready for start up"
	return startDockerContainer(
		expectedOutput,
		"docker", "run", "--rm", "--name", "postgres-emulator", "-p", fmt.Sprintf("127.0.0.1:%d:5432", hostPort),
		"-e", fmt.Sprintf("POSTGRES_PASSWORD=%s", password), postgresDockerImage,
	)
}

func KillPostgresContainer() {
	killDockerContainer("docker", "rm", "--force", "postgres-emulator")
}
