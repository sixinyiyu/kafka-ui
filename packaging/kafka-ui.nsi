; =============================================================================
;  Kafka UI — NSIS 安装包脚本
; -----------------------------------------------------------------------------
;  由 scripts/build-windows.ps1 调用，或手动：
;     makensis /DVERSION=0.1.0 /DOUTDIR=dist /DEXEDIR=dist packaging\kafka-ui.nsi
;
;  期望 EXEDIR 目录中包含 kafka-ui.exe 与 使用说明.txt（由构建脚本准备）。
; =============================================================================

Unicode true
!include "MUI2.nsh"
!include "x64.nsh"

!ifndef VERSION
  !define VERSION "0.1.0"
!endif
!ifndef OUTDIR
  !define OUTDIR "dist"
!endif
!ifndef EXEDIR
  !define EXEDIR "dist"
!endif

!define APP_NAME    "Kafka UI"
!define APP_EXE     "kafka-ui.exe"
!define PUBLISHER   "kafka-ui"
!define REG_UNINST  "Software\Microsoft\Windows\CurrentVersion\Uninstall\KafkaUI"

Name "${APP_NAME} ${VERSION}"
OutFile "${OUTDIR}\kafka-ui-${VERSION}-setup.exe"
InstallDir "$PROGRAMFILES64\${APP_NAME}"
InstallDirRegKey HKLM "${REG_UNINST}" "InstallLocation"
RequestExecutionLevel admin
SetCompressor /SOLID lzma

; ---- 界面 -------------------------------------------------------------------
!define MUI_ABORTWARNING
!define MUI_ICON "${NSISDIR}\Contrib\Graphics\Icons\modern-install.ico"
!define MUI_UNICON "${NSISDIR}\Contrib\Graphics\Icons\modern-uninstall.ico"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "SimpChinese"
!insertmacro MUI_LANGUAGE "English"

; ---- 安装 -------------------------------------------------------------------
Section "Install"
  SetOutPath "$INSTDIR"
  File "${EXEDIR}\${APP_EXE}"
  File /nonfatal "${EXEDIR}\使用说明.txt"

  ; 开始菜单快捷方式
  CreateDirectory "$SMPROGRAMS\${APP_NAME}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\Uninstall.lnk" "$INSTDIR\uninstall.exe"

  ; 桌面快捷方式
  CreateShortcut "$DESKTOP\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE}"

  ; 卸载程序
  WriteUninstaller "$INSTDIR\uninstall.exe"

  ; 注册卸载信息
  WriteRegStr HKLM "${REG_UNINST}" "DisplayName"     "${APP_NAME}"
  WriteRegStr HKLM "${REG_UNINST}" "DisplayVersion"  "${VERSION}"
  WriteRegStr HKLM "${REG_UNINST}" "Publisher"       "${PUBLISHER}"
  WriteRegStr HKLM "${REG_UNINST}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKLM "${REG_UNINST}" "DisplayIcon"     "$INSTDIR\${APP_EXE}"
  WriteRegStr HKLM "${REG_UNINST}" "UninstallString" "$INSTDIR\uninstall.exe"
  WriteRegDWORD HKLM "${REG_UNINST}" "NoModify" 1
  WriteRegDWORD HKLM "${REG_UNINST}" "NoRepair" 1
SectionEnd

; ---- 卸载 -------------------------------------------------------------------
Section "Uninstall"
  Delete "$INSTDIR\${APP_EXE}"
  Delete "$INSTDIR\使用说明.txt"
  Delete "$INSTDIR\uninstall.exe"
  RMDir "$INSTDIR"

  Delete "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\${APP_NAME}\Uninstall.lnk"
  RMDir  "$SMPROGRAMS\${APP_NAME}"
  Delete "$DESKTOP\${APP_NAME}.lnk"

  DeleteRegKey HKLM "${REG_UNINST}"

  ; 询问是否保留用户配置（%APPDATA%\kafka-ui）
  MessageBox MB_YESNO "是否删除用户连接配置（%APPDATA%\kafka-ui）？" IDNO keep_cfg
    RMDir /r "$APPDATA\kafka-ui"
  keep_cfg:
SectionEnd
