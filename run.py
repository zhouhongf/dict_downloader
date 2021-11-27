import subprocess


if __name__ == '__main__':

    servers = [
        # ["pipenv", "run", "gunicorn", "-c", "config/gunicorn.py", "--worker-class", "sanic.worker.GunicornWorker", "server:app"],
        ["pipenv", "run", "python", "server.py"],
        ["pipenv", "run", "python", "client.py"],
    ]

    procs = []
    for server in servers:
        proc = subprocess.Popen(server)
        procs.append(proc)

    for proc in procs:
        proc.wait()
        if proc.poll():
            exit(0)
