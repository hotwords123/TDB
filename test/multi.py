import asyncio
import aioconsole
import argparse
import sys
import re
from pathlib import Path
from typing import Optional, TextIO

class Logger:
    def __init__(self, quiet: bool = False):
        self.quiet = quiet

    def _log(self, level, *args, **kwargs):
        print(f"[{level}]", *args, **kwargs)

    def log(self, level, *args, **kwargs):
        if not self.quiet:
            self._log(level, *args, **kwargs)
    
    def info(self, *args, **kwargs):
        if not self.quiet:
            self._log("info", *args, **kwargs)

    def error(self, *args, **kwargs):
        self._log("error", file=sys.stderr, *args, **kwargs)


class Client:
    def __init__(
        self,
        client_id: str,
        output: Optional[TextIO] = None,
        done_regex: Optional[re.Pattern] = None,
    ):
        self.client_id = client_id
        self.output = output
        self.done_regex = done_regex

        self.state = "idle"
        self.process = None
        self.stdout_task = None
        self.stderr_task = None
        self.done_event = asyncio.Event()

    async def start(self, cmd: str):
        assert self.state == "idle", "Client already started"

        self.process = await asyncio.create_subprocess_shell(
            cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        self.stdout_task = asyncio.create_task(self._read_stream(self.process.stdout, sys.stdout))
        self.stderr_task = asyncio.create_task(self._read_stream(self.process.stderr, sys.stderr))
        self.wait_task = asyncio.create_task(self._wait_cleanup())

        self.state = "running"

    async def wait_cleanup(self):
        assert self.state != "idle", "Client not started"

        await asyncio.wait([self.stdout_task, self.stderr_task, self.wait_task])
        if self.output is not None:
            self.output.close()

    async def send(self, line: str):
        assert self.state == "running", "Client not running"

        if self.done_regex is not None:
            self.done_event.clear()

        self.process.stdin.write(line.encode())
        await self.process.stdin.drain()

        if self.done_regex is not None:
            await self.done_event.wait()

    async def _read_stream(self, stream: asyncio.StreamReader, out: TextIO):
        while True:
            line = await stream.readline()
            line = line.decode()
            if not line:
                self.done_event.set()
                break

            logger.log(f"out:{self.client_id}", line, file=out, end="")
            if self.output:
                self.output.write(line)

            if self.done_regex is not None and self.done_regex.match(line):
                self.done_event.set()

    async def _wait_cleanup(self):
        await self.process.wait()
        self.state = "stopped"
        logger.info(f"client {self.client_id} exited with code {self.process.returncode}")


class App:
    def __init__(self, args):
        self.args = args
        self.output_dir: Optional[Path] = None
        self.done_regex: Optional[re.Pattern] = None
        self.clients: dict[str, Client] = {}
        self.current_client: Optional[Client] = None

        if self.args.output:
            self.output_dir = Path(self.args.output)
            self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.args.done_regex:
            self.done_regex = re.compile(self.args.done_regex)

    @staticmethod
    async def _read_stdin():
        while True:
            line = await aioconsole.ainput("> ")
            yield line

    @staticmethod
    async def _read_file(filename: str, auto: bool = False):
        with open(filename, "r") as f:
            for line in f:
                line = line.removesuffix("\n")
                if auto:
                    print(f"> {line}")
                else:
                    await aioconsole.ainput(f"> {line}")
                yield line

    async def run(self):
        if self.args.input:
            lines = self._read_file(self.args.input, auto=self.args.yes)
        else:
            lines = self._read_stdin()

        async for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith(self.args.marker):
                client_id = line.removeprefix(self.args.marker).strip()
                await self.switch_client(client_id)
            else:
                await self.send_command(line)

        for client in self.clients.values():
            if client.state == "running":
                await client.wait_cleanup()

    async def switch_client(self, client_id: str):
        if client_id not in self.clients:
            logger.info(f"starting client {client_id}")

            output = None
            if self.output_dir is not None:
                output = (self.output_dir / f"{client_id}.log").open("w")

            client = Client(client_id, output=output, done_regex=self.done_regex)
            await client.start(" ".join(self.args.command))
            self.clients[client_id] = client

        self.current_client = self.clients[client_id]
        logger.info(f"switched to client {client_id}")

    async def send_command(self, command: str):
        if self.current_client is None:
            logger.error("no client selected")
            return

        if self.current_client.state != "running":
            logger.error(f"client {self.current_client.client_id} not running")
            return

        await self.current_client.send(command + "\n")

        if self.args.timeout:
            await asyncio.sleep(self.args.timeout)


async def main():
    parser = argparse.ArgumentParser(description="Run multiple clients in parallel")

    parser.add_argument("command", nargs="+", help="Command to run")
    parser.add_argument("-i", "--input", type=str, help="Input commands")
    parser.add_argument("-o", "--output", type=str, help="Output directory")
    parser.add_argument("-t", "--timeout", type=int, default=0.1, help="Timeout between commands")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode")
    parser.add_argument("--marker", type=str, default="-- use", help="Marker for switching clients")
    parser.add_argument("--done-regex", type=str, default=r"^Cost time: \d+", help="Regex to match end of command")

    args = parser.parse_args()

    global logger
    logger = Logger(quiet=args.quiet)

    app = App(args)
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
