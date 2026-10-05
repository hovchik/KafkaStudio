# Installer

`KafkaStudio-Setup-<version>.exe` installs Kafka Studio on Windows 10/11 (64-bit). Nothing else is needed
first: .NET comes inside.

## What the user sees

1. Where to install (default `C:\Program Files\Kafka Studio`).
2. Optional tasks, both unticked by default:
   - *Create a desktop shortcut*.
   - *Add the kafkastudio test runner to the system PATH*, so `kafkastudio test ...` works from any terminal
     or CI agent.
3. **Launch Kafka Studio** at the end.

Start menu → Kafka Studio also has *Kafka Studio command line* (a console showing `kafkastudio --help`).

Upgrading: run the newer installer; it closes the app first. Connections, saved messages and scripts live in
`%APPDATA%\KafkaStudio` and are never touched, by upgrades or by uninstalling. Uninstalling removes the program,
its shortcuts and its PATH entry.

Unattended: `KafkaStudio-Setup-1.2.0.exe /VERYSILENT /TASKS="desktopicon,addtopath"`.

## Building it

On Windows, with the .NET 10 SDK. The script also needs Inno Setup 6.3+ or 7; if it can't find it, it installs it
with winget the first time (or get it from https://jrsoftware.org/isdl.php, or pass `-Iscc <path to ISCC.exe>`):

```powershell
.\installer\build-installer.ps1 -Version 1.2.0
```

The result is `installer\out\KafkaStudio-Setup-1.2.0.exe`. GitHub Actions builds the same file on every pull
request that touches `src/` or the installer, and on tags like `v1.2.0` (*Windows installer* workflow →
artifacts).

The installer isn't code-signed, so Windows SmartScreen shows *"Windows protected your PC"* the first time;
*More info → Run anyway*. Signing needs a code-signing certificate (`SignTool=` in `KafkaStudio.iss`).
