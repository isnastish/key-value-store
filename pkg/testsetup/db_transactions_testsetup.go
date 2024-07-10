package testsetup

var postgresDockerImage = "postgres:16.3"

func StartPostgresContainer() (bool, error) {
	expectedOutput := "database system is ready to accept connections"
	return startDockerContainer(expectedOutput, "docker", "run", "--rm", "--name", "postgres-emulator", "--env", "POSTGRES_PASSWORD=12345", "-p", "4040:5432", "--detach", postgresDockerImage)
}

func KillPostgresContainer() {
	killDockerContainer("docker", "rm", "--force", "posgres-emulator")
}
