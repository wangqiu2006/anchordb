@echo off
chcp 65001 >nul
set "RUSTUP_HOME=G:\rust-toolchain\rustup"
set "CARGO_HOME=G:\rust-toolchain\cargo"
set "PATH=%CARGO_HOME%\bin;%PATH%"
set "DB_PATH=%~1"
if "%DB_PATH%"=="" set "DB_PATH=G:\anchordb\demo.db"

cd /d G:\anchordb
if not exist "target\debug\anchordb.exe" (
    cargo build -p anchordb --features mode-server
)
"G:\anchordb\target\debug\anchordb.exe" shell "%DB_PATH%"
pause
