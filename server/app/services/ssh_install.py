import os
import posixpath
from typing import Dict, Any

import paramiko

AGENT_BUNDLE_DIR = "/opt/pgai/agent"  # внутри server container

def _connect(host: str, port: int, username: str, auth: Dict[str, Any]) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if auth["type"] == "password":
        client.connect(hostname=host, port=port, username=username, password=auth["password"], timeout=15)
        return client

    # private_key (PEM)
    key_text = auth["private_key"]
    if not key_text:
        raise ValueError("private_key is required for auth.type=private_key")

    pkey = None
    # try RSA then Ed25519
    try:
        pkey = paramiko.RSAKey.from_private_key_file(_write_temp_key(key_text))
    except Exception:
        pkey = paramiko.Ed25519Key.from_private_key_file(_write_temp_key(key_text))

    client.connect(hostname=host, port=port, username=username, pkey=pkey, timeout=15)
    return client

def _write_temp_key(key_text: str) -> str:
    # paramiko wants a file path
    path = "/tmp/pgai_tmp_key.pem"
    with open(path, "w", encoding="utf-8") as f:
        f.write(key_text)
    os.chmod(path, 0o600)
    return path

def _exec(ssh: paramiko.SSHClient, cmd: str) -> str:
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    code = stdout.channel.recv_exit_status()
    if code != 0:
        raise RuntimeError(f"SSH command failed ({code}): {cmd}\n{err}\n{out}")
    return out

def _sftp_put_dir(sftp: paramiko.SFTPClient, local_dir: str, remote_dir: str):
    # ensure remote dir exists
    parts = remote_dir.strip("/").split("/")
    cur = "/"
    for p in parts:
        cur = posixpath.join(cur, p)
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)

    for root, dirs, files in os.walk(local_dir):
        rel = os.path.relpath(root, local_dir)
        rdir = remote_dir if rel == "." else posixpath.join(remote_dir, rel.replace("\\", "/"))
        # ensure subdirs
        for d in dirs:
            rd = posixpath.join(rdir, d)
            try:
                sftp.stat(rd)
            except FileNotFoundError:
                sftp.mkdir(rd)
        for fn in files:
            lp = os.path.join(root, fn)
            rp = posixpath.join(rdir, fn)
            sftp.put(lp, rp)

def install_agent_over_ssh(
    host: str,
    ssh_user: str,
    ssh_port: int,
    ssh_auth: Dict[str, Any],
    env_map: Dict[str, str],
) -> Dict[str, Any]:
    """
    Deploys agent to remote host under /opt/pgai/agent and starts container.
    Assumes docker is installed on remote host (MVP).
    """
    ssh = _connect(host, ssh_port, ssh_user, ssh_auth)
    try:
        # 1) ensure target dir
        _exec(ssh, "sudo mkdir -p /opt/pgai && sudo chown -R $(whoami):$(whoami) /opt/pgai")

        # 2) upload agent bundle
        sftp = ssh.open_sftp()
        try:
            # clean old
            try:
                _exec(ssh, "sudo rm -rf /opt/pgai/agent && sudo mkdir -p /opt/pgai/agent && sudo chown -R $(whoami):$(whoami) /opt/pgai/agent")
            except Exception:
                pass
            _sftp_put_dir(sftp, AGENT_BUNDLE_DIR, "/opt/pgai/agent")
        finally:
            sftp.close()

        # 3) write .env
        env_lines = "\n".join([f"{k}={v}" for k, v in env_map.items()]) + "\n"
        sftp = ssh.open_sftp()
        try:
            with sftp.file("/opt/pgai/agent/.env", "w") as f:
                f.write(env_lines)
        finally:
            sftp.close()

        # 4) build & run docker container
        # Dockerfile в agent/ уже есть
        _exec(ssh, "cd /opt/pgai/agent && sudo docker build -t pgai-agent:latest .")

        # stop old container if exists
        _exec(ssh, "sudo docker rm -f pgai-agent || true")

        agent_port = env_map.get("AGENT_PORT", "8010")
        _exec(
            ssh,
            f"sudo docker run -d --restart unless-stopped --name pgai-agent "
            f"-p {agent_port}:{agent_port} --env-file /opt/pgai/agent/.env pgai-agent:latest"
        )

        # 5) health check (local on remote host)
        _exec(ssh, f"curl -fsS http://127.0.0.1:{agent_port}/databases >/dev/null")

        return {"ok": True}
    finally:
        ssh.close()
