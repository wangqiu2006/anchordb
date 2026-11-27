@echo off
chcp 65001 >nul
set "RUSTUP_HOME=G:\rust-toolchain\rustup"
set "CARGO_HOME=G:\rust-toolchain\cargo"
set "PATH=%CARGO_HOME%\bin;%PATH%"
set "DB_PATH=%~1"
if "%DB_PATH%"=="" set "DB_PATH=G:\anchordb\demo.db"

cd /d G:\anchordb
if not exist "target\debug\anchordb.exe" (
    echo Building AnchorDB server...
    cargo build -p anchordb --features mode-server
)
echo Starting AnchorDB server on 127.0.0.1:5432 (%DB_PATH%)...
"G:\anchordb\target\debug\anchordb.exe" serve "%DB_PATH%" --listen 127.0.0.1:5432
pause
