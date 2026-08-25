@echo off
REM Opens one window per component so you can watch them all during the demo.
REM Close a node's window to simulate that server failing.

cd /d "%~dp0"

start "balancer"  cmd /k node src/balancer/balancer.js
timeout /t 1 >nul
start "node1"     cmd /k node src/server/node.js 1
start "node2"     cmd /k node src/server/node.js 2
start "node3"     cmd /k node src/server/node.js 3
timeout /t 1 >nul
start "dashboard" cmd /k node src/dashboard/dashboard.js
timeout /t 2 >nul

start http://127.0.0.1:8080
echo All components started. Dashboard: http://127.0.0.1:8080
