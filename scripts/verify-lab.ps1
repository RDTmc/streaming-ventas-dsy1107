<#
.SYNOPSIS
  Verifica el estado del laboratorio Kafka EA3 (4 EC2) desde Windows.
.DESCRIPTION
  Comprueba instancias AWS, contenedores/health, endpoints JMX, clúster Kafka,
  app (producer + Streams), Prometheus/Grafana/Loki, targets, reglas y logs.
  Uso:
    powershell -ExecutionPolicy Bypass -File .\scripts\verify-lab.ps1
#>
param(
  [string]$KeyPath = $(if ($env:LAB_KEY_PATH) { $env:LAB_KEY_PATH } else { Join-Path (Split-Path -Parent $PSScriptRoot) "kafka-1.pem" }),
  [string]$Kafka1 = "107.22.182.208",
  [string]$Kafka2 = "3.233.42.157",
  [string]$App = "54.173.211.212",
  [string]$Monitoring = "98.82.45.103"
)

$script:pass = 0
$script:fail = 0
$script:warn = 0

function Check([string]$Name, [bool]$Ok, [string]$Detail = "") {
  if ($Ok) {
    Write-Host ("  PASS  {0} {1}" -f $Name, $Detail) -ForegroundColor Green
    $script:pass++
  } else {
    Write-Host ("  FAIL  {0} {1}" -f $Name, $Detail) -ForegroundColor Red
    $script:fail++
  }
}

function Warn([string]$Name, [string]$Detail = "") {
  Write-Host ("  WARN  {0} {1}" -f $Name, $Detail) -ForegroundColor Yellow
  $script:warn++
}

function ToInt([string]$Value) {
  $number = 0
  if ([int]::TryParse(($Value -as [string]).Trim(), [ref]$number)) { return $number }
  return 0
}

function Section([string]$Title) {
  Write-Host ""
  Write-Host "== $Title ==" -ForegroundColor Cyan
}

function Remote([string]$Ip, [string]$Cmd) {
  return ((ssh -i $KeyPath -o ConnectTimeout=20 -o StrictHostKeyChecking=accept-new "ubuntu@$Ip" $Cmd 2>&1) -join "`n").Trim()
}

function HttpCode([string]$Ip, [string]$Url) {
  $c = Remote $Ip "curl -s -o /dev/null -w '%{http_code}' --max-time 5 $Url"
  return ($c -split "`n" | Select-Object -Last 1).Trim()
}

function ContainerStatus([string]$Ip, [string]$Name) {
  return (Remote $Ip "docker inspect -f '{{.State.Health.Status}}' $Name 2>/dev/null || docker inspect -f '{{.State.Status}}' $Name 2>/dev/null || echo missing").Trim()
}

# ---------------------------------------------------------------- AWS
Section "AWS - instancias"
if (Get-Command aws -ErrorAction SilentlyContinue) {
  $states = (aws ec2 describe-instances --region us-east-1 --filters "Name=tag:Name,Values=kafka-1,kafka-2,app,monitoring" "Name=instance-state-name,Values=running" --query 'Reservations[].Instances[].State.Name' --output text 2>&1) -join " "
  $running = ([regex]::Matches($states, "running")).Count
  Check "EC2 running" ($running -eq 4) "($running/4)"
} else {
  Warn "AWS CLI no disponible; se omiten las instancias"
}

# ---------------------------------------------------------------- Kafka 1
Section "kafka-1 - ZooKeeper + broker 1"
Check "zookeeper healthy" ((ContainerStatus $Kafka1 "zookeeper") -eq "healthy") (ContainerStatus $Kafka1 "zookeeper")
Check "kafka healthy" ((ContainerStatus $Kafka1 "kafka") -eq "healthy") (ContainerStatus $Kafka1 "kafka")
Check "node-exporter 9100" ((HttpCode $Kafka1 "http://10.10.0.33:9100/metrics") -eq "200")
Check "JMX kafka 9404" ((HttpCode $Kafka1 "http://10.10.0.33:9404/metrics") -eq "200")
Check "JMX zookeeper 9405" ((HttpCode $Kafka1 "http://10.10.0.33:9405/metrics") -eq "200")
$ui = HttpCode $Kafka1 "http://127.0.0.1:8080/"
Check "Kafka UI 8080" ($ui -in @("200", "302")) "(HTTP $ui)"
$brokers = Remote $Kafka1 "cd ~/lab/broker && docker compose -f docker-compose.broker.yaml --env-file broker-a.env exec -T kafka env -u JAVA_TOOL_OPTIONS kafka-broker-api-versions --bootstrap-server 10.10.0.33:9092 2>/dev/null | grep -c 'rack: null'"
Check "2 brokers registrados" ($brokers.Trim() -eq "2") "($($brokers.Trim())/2)"
$topics = Remote $Kafka1 "cd ~/lab/broker && docker compose -f docker-compose.broker.yaml --env-file broker-a.env exec -T kafka env -u JAVA_TOOL_OPTIONS kafka-topics --bootstrap-server 10.10.0.33:9092 --list 2>/dev/null | grep -cE '^ventas-(raw|agregadas|raw-dlq)$'"
Check "topics de negocio" ($topics.Trim() -eq "3") "($($topics.Trim())/3)"
$disk = Remote $Kafka1 "df --output=pcent / | tail -n 1"
Check "disco kafka-1 < 85%" (([int]($disk -replace '[^0-9]', '')) -lt 85) $disk.Trim()

# ---------------------------------------------------------------- Kafka 2
Section "kafka-2 - broker 2"
Check "kafka healthy" ((ContainerStatus $Kafka2 "kafka") -eq "healthy") (ContainerStatus $Kafka2 "kafka")
Check "node-exporter 9100" ((HttpCode $Kafka2 "http://10.10.0.126:9100/metrics") -eq "200")
Check "JMX kafka 9404" ((HttpCode $Kafka2 "http://10.10.0.126:9404/metrics") -eq "200")
$disk = Remote $Kafka2 "df --output=pcent / | tail -n 1"
Check "disco kafka-2 < 85%" (([int]($disk -replace '[^0-9]', '')) -lt 85) $disk.Trim()

# ---------------------------------------------------------------- App
Section "app - producer + Kafka Streams"
Check "producer running" ((ContainerStatus $App "ventas-app-producer-1") -eq "running") (ContainerStatus $App "ventas-app-producer-1")
Check "streams running" ((ContainerStatus $App "ventas-app-consumer-1") -eq "running") (ContainerStatus $App "ventas-app-consumer-1")
Check "JMX producer 9406" ((HttpCode $App "http://10.10.0.25:9406/metrics") -eq "200")
Check "JMX streams 9407" ((HttpCode $App "http://10.10.0.25:9407/metrics") -eq "200")
$events = Remote $App "docker logs --since 60s ventas-app-producer-1 2>&1 | grep -c eventId"
Check "producer emitiendo (60 s)" ([int]$events -gt 0) "($events eventos)"
$running = Remote $App "docker logs --tail=4000 ventas-app-consumer-1 2>&1 | grep 'State transition' | tail -n 1"
Check "Streams en RUNNING" ($running -match "to RUNNING")

# ---------------------------------------------------------------- Monitoring
Section "monitoring - Prometheus + Grafana + Loki"
foreach ($c in @("prometheus", "grafana", "loki", "node-exporter")) {
  Check "$c running" ((ContainerStatus $Monitoring $c) -eq "running") (ContainerStatus $Monitoring $c)
}
Check "prometheus ready" ((HttpCode $Monitoring "http://127.0.0.1:9090/-/ready") -eq "200")
Check "grafana health" ((HttpCode $Monitoring "http://127.0.0.1:3000/api/health") -eq "200")
$loki = HttpCode $Monitoring "http://10.10.0.61:3100/ready"
if ($loki -eq "200") { Check "loki ready" $true }
else { Warn "loki ready" "(HTTP $loki; si el host acaba de arrancar, esperar 1-2 min)" }
$up = Remote $Monitoring "curl -s http://127.0.0.1:9090/api/v1/targets | grep -o 'health.:.up' | wc -l"
Check "targets up" ($up.Trim() -eq "10") "($($up.Trim())/10)"
$rules = Remote $Monitoring "curl -s http://127.0.0.1:9090/api/v1/rules | grep -o 'health.:.' | wc -l"
$firing = Remote $Monitoring "curl -s http://127.0.0.1:9090/api/v1/rules | grep -o 'state.:.firing' | wc -l"
Check "reglas cargadas" ($rules.Trim() -eq "10" -and $firing.Trim() -eq "0") "($($rules.Trim())/10, firing=$($firing.Trim()))"
$nodes = Remote $Monitoring "curl -s http://10.10.0.61:3100/loki/api/v1/label/node/values"
Check "logs de 4 nodos en Loki" ($nodes -match "kafka-zookeeper" -and $nodes -match "kafka-broker-2" -and $nodes -match '"app"' -and $nodes -match "monitoring")
$disk = Remote $Monitoring "df --output=pcent / | tail -n 1"
Check "disco monitoring < 85%" (([int]($disk -replace '[^0-9]', '')) -lt 85) $disk.Trim()

# ---------------------------------------------------------------- Warnings y resumen
Section "RESUMEN"
$mon = Remote $Monitoring "docker logs --since 10m grafana-alloy 2>&1 | grep -ciE 'level=error|panic'"
if ((ToInt $mon) -gt 0) { Warn "Alloy (monitoring) con errores recientes" "($mon)" }
foreach ($pair in @(@("kafka-1", $Kafka1), @("kafka-2", $Kafka2), @("app", $App))) {
  $n = Remote $pair[1] "docker logs --since 10m grafana-alloy 2>&1 | grep -ciE 'level=error|panic'"
  if ((ToInt $n) -gt 0) { Warn "Alloy ($($pair[0])) con errores recientes" "($n)" }
}

Write-Host ""
Write-Host ("PASS: {0}   FAIL: {1}   WARN: {2}" -f $script:pass, $script:fail, $script:warn) -ForegroundColor $(if ($script:fail -eq 0) { "Green" } else { "Red" })
if ($script:fail -eq 0) {
  Write-Host "Laboratorio SANO ✅" -ForegroundColor Green
  exit 0
} else {
  Write-Host "Hay comprobaciones fallidas ❌" -ForegroundColor Red
  exit 1
}
