; Kafka Studio installer (Inno Setup 6.3+ or 7).
;
; Build it with build-installer.ps1 next to this file; it publishes the app and the CLI and passes AppVersion,
; PublishDir and CliDir. By hand:
;   ISCC.exe /DAppVersion=1.2.0 /DPublishDir=out\publish\app /DCliDir=out\publish\cli KafkaStudio.iss
;
; What it does:
;   - installs a self-contained build (no .NET to install) into Program Files
;   - Start menu and optional desktop shortcuts
;   - installs the headless test runner (kafkastudio.exe) and optionally adds it to the system PATH
;   - opens the app at the end
; The user's data (%APPDATA%\KafkaStudio: connections, saved messages, scripts) is never touched, so upgrades
; and uninstalls keep it.

#ifndef AppVersion
  #define AppVersion "1.0.0"
#endif
#ifndef PublishDir
  #define PublishDir "out\publish\app"
#endif
#ifndef CliDir
  #define CliDir "out\publish\cli"
#endif
#define AppName "Kafka Studio"
#define AppExe "KafkaStudio.App.exe"

[Setup]
; Keep this id forever: it is how a new version finds and upgrades the installed one.
AppId={{8E02FA4C-E773-4738-960B-7A21C9204C59}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppName}
VersionInfoVersion={#AppVersion}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
DisableDirPage=auto
; Program Files and the system PATH need an administrator.
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
; Tell Windows the environment changed when PATH is updated.
ChangesEnvironment=yes
CloseApplications=force
RestartApplications=no
SetupIconFile=..\src\KafkaStudio.App\Assets\app.ico
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName}
WizardStyle=modern
Compression=lzma2/max
SolidCompression=yes
OutputDir=out
OutputBaseFilename=KafkaStudio-Setup-{#AppVersion}

[Languages]
Name: "en"; MessagesFile: "compiler:Default.isl"

[CustomMessages]
en.CliGroup=Command line:
en.TaskPath=Add the kafkastudio test runner to the system PATH
en.CliHelp=Kafka Studio command line

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "addtopath"; Description: "{cm:TaskPath}"; GroupDescription: "{cm:CliGroup}"; Flags: unchecked

[Files]
Source: "{#PublishDir}\*"; DestDir: "{app}"; Excludes: "*.pdb"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#CliDir}\*"; DestDir: "{app}\cli"; Excludes: "*.pdb"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "..\README.md"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExe}"
Name: "{group}\{cm:CliHelp}"; Filename: "{cmd}"; Parameters: "/k ""{app}\cli\kafkastudio.exe"" --help"; WorkingDir: "{userdocs}"
Name: "{group}\{cm:UninstallProgram,{#AppName}}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; Tasks: desktopicon

[Registry]
; Append {app}\cli to the machine PATH (only if it isn't there yet); removed again on uninstall in [Code].
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; \
  ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}\cli"; \
  Tasks: addtopath; Check: NeedsAddPath(ExpandConstant('{app}\cli'))

[Run]
; Open the app as the person who ran setup (not the elevated admin), so their data lands in their own profile.
Filename: "{app}\{#AppExe}"; Description: "{cm:LaunchProgram,{#AppName}}"; Flags: nowait postinstall skipifsilent runasoriginaluser

[Code]
const
  EnvKey = 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment';

function NeedsAddPath(Dir: string): Boolean;
var
  Path: string;
begin
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE, EnvKey, 'Path', Path) then
  begin
    Result := True;
    exit;
  end;
  Result := Pos(';' + Uppercase(Dir) + ';', ';' + Uppercase(Path) + ';') = 0;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  Path, Dir: string;
  P: Integer;
begin
  if CurUninstallStep <> usPostUninstall then exit;
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE, EnvKey, 'Path', Path) then exit;
  Dir := ExpandConstant('{app}\cli');
  P := Pos(';' + Uppercase(Dir) + ';', ';' + Uppercase(Path) + ';');
  if P = 0 then exit;
  { P is 1-based in the ';'-padded string, so it is the index of the entry's leading ';' in Path (or 1). }
  if P = 1 then
    Delete(Path, 1, Length(Dir) + 1)
  else
    Delete(Path, P - 1, Length(Dir) + 1);
  RegWriteExpandStringValue(HKEY_LOCAL_MACHINE, EnvKey, 'Path', Path);
end;
