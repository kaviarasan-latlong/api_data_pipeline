"""
config_loader.py

Loads config.yaml and resolves ${ENV_VAR} placeholders against os.environ.
Every other module imports `load_config()` from here rather than reading
YAML directly, so there is exactly one place that knows about the file
layout / env var resolution.
"""

import os
import re
from pathlib import Path

import yaml

_ENV_VAR_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"


def _resolve_env_vars(value):
    """Recursively walk the parsed YAML structure and substitute ${VAR}."""
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    if isinstance(value, str):
        match = _ENV_VAR_RE.fullmatch(value)
        if match:
            env_name = match.group(1)
            resolved = os.environ.get(env_name)
            if resolved is None:
                raise EnvironmentError(
                    f"Config references ${{{env_name}}} but that environment "
                    f"variable is not set."
                )
            return resolved
        return value
    return value


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    return _resolve_env_vars(raw)


# --- Convenience accessors -------------------------------------------------

def get_db_dsn(cfg: dict) -> str:
    db = cfg["database"]
    return (
        f"host={db['host']} port={db['port']} dbname={db['dbname']} "
        f"user={db['user']} password={db['password']} "
        f"connect_timeout={db.get('connect_timeout', 10)}"
    )


def get_included_api_types(cfg: dict) -> list[str]:
    return cfg["pipeline"]["included_api_types"]


def get_columns(cfg: dict) -> dict:
    return cfg["pipeline"]["columns"]


def get_tables(cfg: dict) -> dict:
    return cfg["tables"]


if __name__ == "__main__":
    import json
    print(json.dumps(load_config(), indent=2))
