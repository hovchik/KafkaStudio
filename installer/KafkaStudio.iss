; KafkaStudio Windows installer (Inno Setup 6 script).
;
; Prerequisites (one-time, on the machine building the installer):
;   - Inno Setup 6: https://jrsoftware.org/isdl.php
;
; Usage:
;   1. Publish the app first (self-contained, so end users don't need the .NET runtime):
;        dotnet publish src\KafkaStudio.App\KafkaStudio.App.csproj -c Release -r win-x64 ^
;            --self-contained true -p:PublishSingleFile=false -o publish\win-x64
;      (or just run tools\Publish.ps1, which does this for you)
;   2. Compile this script with Inno Setup:
;        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" installer\KafkaStudio.iss
;   3. The installer is written to installer\output\KafkaStudioSetup-<version>.exe
;
#define AppName "KafkaStudio"
#define AppVersion GetEnv("KAFKASTUDIO_VERSION")
#if AppVersion == ""
  #define AppVersion "1.0.0"
#endif
#define AppPublisher "KafkaStudio"
#define AppExeName "KafkaStudio.App.exe"
#define PublishDir "..\publish\win-x64"

[Setup]
AppId={{7C6E0E3E-2C39-4C1C-9E4F-3E7C8D1B7B5A}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
UninstallDisplayIcon={app}\{#AppExeName}
OutputDir=output
OutputBaseFilename=KafkaStudioSetup-{#AppVersion}
Compression=lzma2
SolidCompression=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
SetupIconFile=..\src\KafkaStudio.App\Assets\app.ico
WizardStyle=modern
DisableProgramGroupPage=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

[Files]
Source: "{#PublishDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExeName}"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Launch {#AppName}"; Flags: nowait postinstall skipifsilent
