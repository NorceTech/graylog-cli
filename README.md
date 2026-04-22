# graylog-cli

Command-line interface for Graylog.

## Install

Download the latest binary for your platform from [Releases](https://github.com/NorceTech/graylog-cli/releases/latest).

### macOS (Apple Silicon)

```sh
curl -sL https://github.com/NorceTech/graylog-cli/releases/latest/download/graylog-cli-macos-aarch64 -o /usr/local/bin/graylog-cli
chmod +x /usr/local/bin/graylog-cli
```

### Linux (x86_64)

```sh
curl -sL https://github.com/NorceTech/graylog-cli/releases/latest/download/graylog-cli-linux-x86_64 -o /usr/local/bin/graylog-cli
chmod +x /usr/local/bin/graylog-cli
```

### Windows (x86_64)

```powershell
$installDir = Join-Path $env:LOCALAPPDATA 'Programs\graylog-cli'
New-Item -ItemType Directory -Path $installDir -Force | Out-Null
Invoke-WebRequest `
    -Uri 'https://github.com/NorceTech/graylog-cli/releases/latest/download/graylog-cli-windows-x86_64.exe' `
    -OutFile (Join-Path $installDir 'graylog-cli.exe') `
    -UseBasicParsing

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not (($userPath -split ';') -contains $installDir)) {
    [Environment]::SetEnvironmentVariable(
        'Path',
        ($userPath.TrimEnd(';') + ';' + $installDir),
        'User'
    )
}
```

## Usage

### Authenticate

```sh
graylog-cli auth --url https://graylog.example.com --token <your-access-token>
```

Credentials are persisted locally for subsequent commands.

```sh
graylog-cli --help
graylog-cli <command> --help
```
