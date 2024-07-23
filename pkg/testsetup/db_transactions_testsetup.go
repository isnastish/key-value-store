package testsetup

var postgresDockerImage = "postgres:16.3"

func StartPostgresContainer() (bool, error) {
	expectedOutput := "PostgreSQL init process complete; ready for start up"
	return startDockerContainer(
		expectedOutput,
		"docker", "run", "--rm", "--name", "postgres-emulator", "-p", "127.0.0.1:4040:5432", "-e", "POSTGRES_PASSWORD=12345", postgresDockerImage)
}

func KillPostgresContainer() {
	killDockerContainer("docker", "rm", "--force", "postgres-emulator")
}
