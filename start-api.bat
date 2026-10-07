@echo off
REM Talk2 API - install deps, build, start (for human testing).
REM Usage: double-click or run start-api.bat
REM Needs: Go 1.22+ and a .env file (copy from .env.example).
setlocal
cd /d "%~dp0"

REM --- Find Go (PATH or default install location) ---
where go >nul 2>nul
if %errorlevel% neq 0 (
  if exist "C:\Program Files\Go\bin\go.exe" (
    set "PATH=C:\Program Files\Go\bin;%PATH%"
  ) else (
    echo [ERROR] Go not found. Install it from https://go.dev/dl/ and re-run.
    exit /b 1
  )
)

if "%PORT%"=="" (
  REM Pick up PORT from .env when set (env var wins if already defined).
  for /f "usebackq tokens=1* delims==" %%a in (".env") do if "%%a"=="PORT" set PORT=%%b
)
if "%PORT%"=="" set PORT=8080

@echo off
REM Talk2 API - free the port, install deps, build, start (for human testing).
REM Usage: double-click or run start-api.bat
REM Needs: Go 1.22+ and a .env file (copy from .env.example).
setlocal
cd /d "%~dp0"

REM --- Find Go (PATH or default install location) ---
where go >nul 2>nul
if %errorlevel% neq 0 (
  if exist "C:\Program Files\Go\bin\go.exe" (
    set "PATH=C:\Program Files\Go\bin;%PATH%"
  ) else (
    echo [ERROR] Go not found. Install it from https://go.dev/dl/ and re-run.
    exit /b 1
  )
)

if "%PORT%"=="" (
  REM Pick up PORT from .env when set (env var wins if already defined).
  for /f "usebackq tokens=1* delims==" %%a in (".env") do if "%%a"=="PORT" set PORT=%%b
)
if "%PORT%"=="" set PORT=8080

echo [1/4] Freeing port %PORT% if occupied...
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
  echo   Killing PID %%p on port %PORT%...
  taskkill /F /PID %%p >nul 2>nul
)
timeout /t 2 /nobreak >nul

echo [2/4] Installing dependencies...
go mod tidy
if %errorlevel% neq 0 (
  echo [ERROR] go mod tidy failed.
  exit /b 1
)

echo [3/4] Building api...
go build -o talk2-api.exe ./cmd/api
if %errorlevel% neq 0 (
  echo [ERROR] build failed.
  exit /b 1
)

if not exist ".env" (
  echo [WARN] No .env file found. Copy .env.example to .env and fill in secrets.
  echo [WARN] Server will still start, but /readyz will report degraded.
)

echo [4/4] Starting talk2-api on port %PORT%...
echo Open these links to test (Ctrl+click in most terminals):
echo   Health:  http://localhost:%PORT%/healthz
echo   Ready:   http://localhost:%PORT%/readyz
echo   Info:    http://localhost:%PORT%/
echo Press Ctrl+C to stop.
talk2-api.exe
