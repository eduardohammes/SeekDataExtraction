from invoke import task

@task
def lint(c):
    """Run code linters and formatters."""
    c.run('pre-commit run --all-files')

@task
def test(c):
    """Run unit tests."""
    c.run('pytest tests/')

@task
def start(c):
    """Start the Docker containers."""
    c.run('docker-compose up -d')

@task
def stop(c):
    """Stop the Docker containers."""
    c.run('docker-compose down')

@task
def build_docs(c):
    """Build documentation (if using a documentation tool)."""
    c.run('mkdocs build')
