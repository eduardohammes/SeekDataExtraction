from invoke import task


@task
def lint(c):
    """Executa os linters e formatadores."""
    c.run("pre-commit run --all-files")


@task
def test(c):
    """Executa os testes unitários."""
    c.run("pytest tests/")


@task
def install_hooks(c):
    """Instala os hooks do pre-commit."""
    c.run("pre-commit install")


@task
def start(c):
    """Inicia os contêineres Docker."""
    c.run("docker-compose up -d")


@task
def stop(c):
    """Para os contêineres Docker."""
    c.run("docker-compose down")


# Outras tarefas existentes...
