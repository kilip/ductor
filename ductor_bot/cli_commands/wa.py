"""WhatsApp management CLI subcommands (``ductor wa ...``)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from collections.abc import Callable

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ductor_bot.workspace.paths import resolve_paths

_console = Console()

_WA_SUBCOMMANDS = frozenset({"setup", "auth", "logout", "status"})


def _parse_wa_subcommand(args: list[str]) -> str | None:
    """Extract the subcommand after 'wa' from CLI args."""
    found = False
    for a in args:
        if a.startswith("-"):
            continue
        if not found and a == "wa":
            found = True
            continue
        if found:
            return a if a in _WA_SUBCOMMANDS else None
    return None


def print_wa_help() -> None:
    """Print the wa subcommand help."""
    _console.print()
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column(style="bold green", min_width=30)
    table.add_column()
    table.add_row("ductor wa setup", "Install Node.js dependencies for the sidecar")
    table.add_row("ductor wa auth", "Generate QR code and wait for scan")
    table.add_row("ductor wa logout", "Clear auth state")
    table.add_row("ductor wa status", "Check connection status")

    _console.print(
        Panel(
            table,
            title="WhatsApp Commands",
            border_style="blue",
            padding=(1, 0),
        ),
    )
    _console.print()


def _get_sidecar_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "messenger" / "whatsapp" / "sidecar"


def wa_setup() -> None:
    """Install Node.js dependencies for the sidecar."""
    sidecar_dir = _get_sidecar_dir()
    _console.print(f"Running npm install in {sidecar_dir}...")
    try:
        subprocess.run(["npm", "install"], cwd=str(sidecar_dir), check=True)
        _console.print("Dependencies installed.")
    except subprocess.CalledProcessError as e:
        _console.print(f"[red]Error running npm install: {e}[/red]")


def wa_auth() -> None:
    """Generate QR code and wait for scan."""
    paths = resolve_paths()
    auth_dir = paths.wa_auth_dir
    sidecar_dir = _get_sidecar_dir()
    
    _console.print("Starting Baileys for authentication...")
    _console.print("Wait for the QR code to appear, then scan it with WhatsApp.")
    try:
        subprocess.run(
            ["node", "index.js", str(auth_dir), "auth"],
            cwd=str(sidecar_dir)
        )
    except KeyboardInterrupt:
        _console.print("\nCancelled.")


def wa_logout() -> None:
    """Clear auth state."""
    paths = resolve_paths()
    auth_dir = paths.wa_auth_dir
    sidecar_dir = _get_sidecar_dir()
    
    subprocess.run(
        ["node", "index.js", str(auth_dir), "logout"],
        cwd=str(sidecar_dir)
    )
    _console.print("Logged out and cleared auth state.")


def wa_status() -> None:
    """Check connection status."""
    paths = resolve_paths()
    creds_file = paths.wa_auth_dir / "creds.json"
    
    if creds_file.exists():
        _console.print("[green]WhatsApp is authenticated.[/green]")
    else:
        _console.print("[yellow]WhatsApp is NOT authenticated.[/yellow] Run `ductor wa auth` to log in.")
        
    _console.print("Note: The sidecar daemon only runs when you start the main bot (`ductor`).")


def cmd_wa(args: list[str]) -> None:
    """Handle 'ductor wa <subcommand>'."""
    sub = _parse_wa_subcommand(args)
    if sub is None:
        print_wa_help()
        return

    dispatch: dict[str, Callable[[], None]] = {
        "setup": wa_setup,
        "auth": wa_auth,
        "logout": wa_logout,
        "status": wa_status,
    }
    dispatch[sub]()
