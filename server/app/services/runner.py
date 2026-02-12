from __future__ import annotations

import asyncio
import os
import posixpath
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
import shlex
import httpx
import paramiko


AGENT_BUNDLE_DIR = "/opt/pgai/agent"  # внутри server container (COPY agent -> /opt/pgai/agent)


def _read_all(stdout, stderr) -> tuple[str, str, int]:
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    code = stdout.channel.recv_exit_status()
    return out, err, code


def _ssh_exec(ssh: paramiko.SSHClient, cmd: str, sudo: bool = False, sudo_password: Optional[str] = None) -> str:
    if sudo:
        if sudo_password:
            # sudo with password
            cmd = f"sudo -S bash -lc {shlex.quote(cmd)}"
            stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
            stdin.write(sudo_password + "\n")
            stdin.flush()
        else:
            # sudo without password
            cmd = f"sudo -n bash -lc {shlex.quote(cmd)}"
            stdin, stdout, stderr = ssh.exec_command(cmd)
    else:
        cmd = f"bash -lc {shlex.quote(cmd)}"
        stdin, stdout, stderr = ssh.exec_command(cmd)

    out, err, code = _read_all(stdout, stderr)
    if code != 0:
        raise RuntimeError(f"SSH command failed ({code}): {cmd}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def _sftp_mkdir_p(sftp: paramiko.SFTPClient, remote_dir: str):
    parts = remote_dir.strip("/").split("/")
    cur = "/"
    for p in parts:
        cur = posixpath.join(cur, p)
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)


def _sftp_put_dir(sftp: paramiko.SFTPClient, local_dir: str, remote_dir: str):
    _sftp_mkdir_p(sftp, remote_dir)
    for root, dirs, files in os.walk(local_dir):
        rel = os.path.relpath(root, local_dir)
        rdir = remote_dir if rel == "." else posixpath.join(remote_dir, rel.replace("\\", "/"))
        _sftp_mkdir_p(sftp, rdir)
        for d in dirs:
            _sftp_mkdir_p(sftp, posixpath.join(rdir, d))
        for fn in files:
            lp = os.path.join(root, fn)
            rp = posixpath.join(rdir, fn)
            sftp.put(lp, rp)


def _make_pkey_from_pem(pem_text: str) -> paramiko.PKey:
    # Try RSA then Ed25519 then ECDSA
    bio = StringIO(pem_text)
    try:
        return paramiko.RSAKey.from_private_key(bio)
    except Exception:
        pass
    bio = StringIO(pem_text)
    try:
        return paramiko.Ed25519Key.from_private_key(bio)
    except Exception:
        pass
    bio = StringIO(pem_text)
    return paramiko.ECDSAKey.from_private_key(bio)


def _ssh_connect(host: str, port: int, username: str, auth_type: str, password: Optional[str], private_key: Optional[str]) -> paramiko.SSHClient:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if auth_type == "password":
        if not password:
            raise ValueError("ssh password is required")
        ssh.connect(hostname=host, port=port, username=username, password=password, timeout=20)
        return ssh

    if not private_key:
        raise ValueError("ssh private_key is required")

    pkey = _make_pkey_from_pem(private_key)
    ssh.connect(hostname=host, port=port, username=username, pkey=pkey, timeout=20)
    return ssh


def _detect_sudo(ssh: paramiko.SSHClient, sudo_password: Optional[str]) -> tuple[bool, Optional[str]]:
    """
    Return (sudo_ok, sudo_password_to_use).
    If sudo -n works => passwordless sudo.
    If not, but password is provided => sudo with password.
    """
    try:
        _ssh_exec(ssh, "true", sudo=True, sudo_password=None)
        return True, None
    except Exception:
        if sudo_password:
            # test with password
            _ssh_exec(ssh, "true", sudo=True, sudo_password=sudo_password)
            return True, sudo_password
        return False, None


def _ensure_packages_ubuntu(ssh: paramiko.SSHClient, sudo_password: Optional[str]):
    # python3-venv + pip + curl
    _ssh_exec(ssh, "apt-get update -y", sudo=True, sudo_password=sudo_password)
    _ssh_exec(
        ssh,
        "DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv python3-pip curl ca-certificates",
        sudo=True,
        sudo_password=sudo_password,
    )


def _ensure_user_dirs(ssh: paramiko.SSHClient, sudo_password: Optional[str]):
    _ssh_exec(ssh, "mkdir -p /opt/pgai", sudo=True, sudo_password=sudo_password)
    # Make it owned by root but readable; agent will run as service user (ssh_user)
    _ssh_exec(ssh, "mkdir -p /opt/pgai/agent", sudo=True, sudo_password=sudo_password)
    _ssh_exec(ssh, "chmod 755 /opt/pgai /opt/pgai/agent", sudo=True, sudo_password=sudo_password)


def _write_remote_text(sftp: paramiko.SFTPClient, remote_path: str, text: str):
    parent = posixpath.dirname(remote_path)
    _sftp_mkdir_p(sftp, parent)
    with sftp.file(remote_path, "w") as f:
        f.write(text)


def _install_agent_files(ssh: paramiko.SSHClient, sudo_password: Optional[str]):
    # Clean old agent dir
    _ssh_exec(ssh, "rm -rf /opt/pgai/agent/*", sudo=True, sudo_password=sudo_password)

    # Upload bundle
    sftp = ssh.open_sftp()
    try:
        if not os.path.isdir(AGENT_BUNDLE_DIR):
            raise RuntimeError(f"Agent bundle not found inside server container: {AGENT_BUNDLE_DIR}. Check server/Dockerfile COPY agent -> /opt/pgai/agent")
        _sftp_put_dir(sftp, AGENT_BUNDLE_DIR, "/opt/pgai/agent")
    finally:
        sftp.close()

    _ssh_exec(ssh, "chmod -R 755 /opt/pgai/agent", sudo=True, sudo_password=sudo_password)


def _setup_venv_and_deps(ssh: paramiko.SSHClient, sudo_password: Optional[str]):
    # Create venv
    _ssh_exec(ssh, "python3 -m venv /opt/pgai/agent/.venv", sudo=True, sudo_password=sudo_password)
    # Upgrade pip and install requirements
    _ssh_exec(
        ssh,
        "/opt/pgai/agent/.venv/bin/pip install --upgrade pip",
        sudo=True,
        sudo_password=sudo_password,
    )
    _ssh_exec(
        ssh,
        "/opt/pgai/agent/.venv/bin/pip install -r /opt/pgai/agent/requirements.txt",
        sudo=True,
        sudo_password=sudo_password,
    )


def _write_env_file(ssh: paramiko.SSHClient, sudo_password: Optional[str], env_map: Dict[str, str]):
    content = "\n".join([f"{k}={v}" for k, v in env_map.items()]) + "\n"
    sftp = ssh.open_sftp()
    try:
        _write_remote_text(sftp, "/opt/pgai/agent/.env", content)
    finally:
        sftp.close()
    _ssh_exec(ssh, "chmod 600 /opt/pgai/agent/.env", sudo=True, sudo_password=sudo_password)


def _install_systemd_service(ssh: paramiko.SSHClient, sudo_password: Optional[str], run_user: str):
    service_text = f"""\
[Unit]
Description=pgAI Agent
After=network.target

[Service]
Type=simple
User={run_user}
WorkingDirectory=/opt/pgai/agent
EnvironmentFile=/opt/pgai/agent/.env
ExecStart=/opt/pgai/agent/.venv/bin/python /opt/pgai/agent/agent.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
"""
    sftp = ssh.open_sftp()
    try:
        _write_remote_text(sftp, "/tmp/pgai-agent.service", service_text)
    finally:
        sftp.close()

    _ssh_exec(
        ssh,
        "mv /tmp/pgai-agent.service /etc/systemd/system/pgai-agent.service",
        sudo=True,
        sudo_password=sudo_password,
    )
    _ssh_exec(ssh, "systemctl daemon-reload", sudo=True, sudo_password=sudo_password)
    _ssh_exec(ssh, "systemctl enable --now pgai-agent.service", sudo=True, sudo_password=sudo_password)
    _ssh_exec(ssh, "systemctl restart pgai-agent.service", sudo=True, sudo_password=sudo_password)


def _open_firewall_if_ufw(ssh: paramiko.SSHClient, sudo_password: Optional[str], port: int):
    # best-effort: if ufw exists + active -> allow
    try:
        status = _ssh_exec(ssh, "command -v ufw >/dev/null 2>&1 && ufw status || echo 'no_ufw'", sudo=True, sudo_password=sudo_password)
        if "Status: active" in status:
            _ssh_exec(ssh, f"ufw allow {port}/tcp", sudo=True, sudo_password=sudo_password)
    except Exception:
        # ignore (firewall config can be managed by admin)
        pass


def _remote_health_check(ssh: paramiko.SSHClient, sudo_password: Optional[str], port: int):
    # Wait a bit and check locally on the remote host
    _ssh_exec(ssh, "sleep 1", sudo=False)
    _ssh_exec(ssh, f"curl -fsS http://127.0.0.1:{port}/databases >/dev/null", sudo=True, sudo_password=sudo_password)


def _install_agent_over_ssh_sync(server_ip: str, req) -> str:
    """
    Full install:
    - apt deps
    - upload agent bundle
    - venv + pip install
    - .env
    - systemd service
    - verify
    """
    ssh_password = req.ssh_auth.password if req.ssh_auth.type == "password" else None
    ssh = _ssh_connect(
        host=server_ip,
        port=req.ssh_port,
        username=req.ssh_user,
        auth_type=req.ssh_auth.type,
        password=ssh_password,
        private_key=req.ssh_auth.private_key,
    )
    try:
        sudo_ok, sudo_pw = _detect_sudo(ssh, sudo_password=ssh_password)
        if not sudo_ok:
            raise RuntimeError("SSH user has no sudo access (or sudo password not provided). Need sudo to install agent and systemd service.")

        # Detect OS (we support Ubuntu/Debian via apt; fail otherwise)
        try:
            _ssh_exec(ssh, "command -v apt-get >/dev/null 2>&1", sudo=True, sudo_password=sudo_pw)
        except Exception:
            raise RuntimeError("Target host is not Debian/Ubuntu (apt-get not found). Implement other installers or preinstall python/venv manually.")

        _ensure_packages_ubuntu(ssh, sudo_pw)
        _ensure_user_dirs(ssh, sudo_pw)
        _install_agent_files(ssh, sudo_pw)

        # Ensure the service user can read /opt/pgai/agent
        _ssh_exec(ssh, f"chown -R {req.ssh_user}:{req.ssh_user} /opt/pgai/agent", sudo=True, sudo_password=sudo_pw)

        _setup_venv_and_deps(ssh, sudo_pw)

        pg_dsn = f"postgresql://{req.pg_user}:{req.pg_password}@{req.pg_host}:{req.pg_port}/{req.pg_database}"
        env_map = {
            "PG_DSN": pg_dsn,
            "OPENAI_API_KEY": req.openai_api_key,
            "MODEL": req.model,
            "MAX_STATEMENTS": str(req.max_statements),
            "AGENT_PORT": str(req.agent_port),
        }
        _write_env_file(ssh, sudo_pw, env_map)

        _install_systemd_service(ssh, sudo_pw, run_user=req.ssh_user)
        _open_firewall_if_ufw(ssh, sudo_pw, port=req.agent_port)
        _remote_health_check(ssh, sudo_pw, port=req.agent_port)

        return f"http://{server_ip}:{req.agent_port}"
    finally:
        ssh.close()


# ---- public async API ----

async def install_agent_via_ssh(server_ip: str, req) -> str:
    # paramiko is blocking; run in thread to not freeze event loop
    return await asyncio.to_thread(_install_agent_over_ssh_sync, server_ip, req)


# ---- Agent HTTP calls ----

async def agent_list_databases(agent_url: str) -> List[str]:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{agent_url}/databases")
        r.raise_for_status()
        data = r.json()
        return data.get("databases", [])


async def agent_collect(agent_url: str, databases: List[str], blocks: List[str]) -> Dict[str, Any]:
    payload = {"databases": databases, "blocks": blocks}
    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.post(f"{agent_url}/run", json=payload)
        r.raise_for_status()
        return r.json()
