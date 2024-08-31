package testsetup

import (
	"fmt"
)

func StartPostgresContainer(port int) (bool, error) {
	expectedOutput := "PostgreSQL init process complete; ready for start up"
	return startDockerContainer(
		expectedOutput,
		"docker", "run", "--rm", "--name", "postgres-emulator", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"-e", "POSTGRES_PASSWORD=saml", "postgres:16.3",
	)
}

func KillPostgresContainer() {
	killDockerContainer("docker", "rm", "--force", "postgres-emulator")
}
