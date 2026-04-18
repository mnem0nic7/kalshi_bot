from __future__ import annotations

import asyncio
import json
import logging
import shutil
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)

_CODEX_BINARY = "codex"
_DEFAULT_TIMEOUT = 120.0


class CodexCLIProvider:
    """LLM provider that shells out to the Codex CLI binary.

    The prompt is piped via stdin; the CLI writes only the model's response
    to stdout (header/warnings go to stderr, which we discard).
    Model and auth are resolved by the CLI from ~/.codex/config.toml and
    ~/.codex/auth.json — no API key needed in this process.
    """

    def __init__(self, *, model: str | None = None, timeout_seconds: float = _DEFAULT_TIMEOUT) -> None:
        self.model = model
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def is_available() -> bool:
        return shutil.which(_CODEX_BINARY) is not None

    def _argv(self) -> list[str]:
        # Prompt is always piped via stdin ("-"), never interpolated into args.
        # approval_policy=never prevents interactive confirmation prompts.
        args = [_CODEX_BINARY, "exec", "-c", 'approval_policy="never"']
        if self.model:
            args += ["-m", self.model]
        args.append("-")
        return args

    async def _run(self, prompt: str) -> str:
        proc = await asyncio.create_subprocess_exec(
            *self._argv(),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(input=prompt.encode()),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            raise RuntimeError(f"Codex CLI timed out after {self.timeout_seconds}s")

        if proc.returncode != 0:
            raise RuntimeError(f"Codex CLI exited with code {proc.returncode}")

        return stdout.decode(errors="replace").strip()

    async def complete_text(self, *, system_prompt: str, user_prompt: str, model: str, temperature: float) -> str:
        return await self._run(f"{system_prompt}\n\n{user_prompt}")

    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        schema_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        json_note = (
            "\n\nIMPORTANT: Your entire response must be valid JSON only. "
            "No prose, no markdown, no code fences."
        )
        raw = await self._run(f"{system_prompt}{json_note}\n\n{user_prompt}")

        # Strip accidental markdown fences
        if raw.startswith("```"):
            lines = raw.splitlines()
            raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        decoded = json.loads(raw)
        if schema_model is not None:
            decoded = schema_model.model_validate(decoded).model_dump(mode="json")
        return decoded

    async def close(self) -> None:
        pass
