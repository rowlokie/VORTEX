
Write-Host "=== Testing Kafka Clone ===" -ForegroundColor Green

function Send-Cmd {
    param($Command)
    try {
        Write-Host "Sending: $Command" -ForegroundColor Gray
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("localhost", 9092)
        $stream = $tcp.GetStream()
        $writer = New-Object System.IO.StreamWriter($stream)
        $writer.WriteLine($Command)
        $writer.Flush()
        
        $reader = New-Object System.IO.StreamReader($stream)
        $response = $reader.ReadToEnd()
        
        $tcp.Close()
        Write-Host "Response received!" -ForegroundColor Gray
        return $response
    }
    catch {
        return "ERROR: $_"
    }
}

# Test 1: Create Topic
Write-Host "`n[TEST 1] Creating topic 'orders' with 3 partitions..." -ForegroundColor Yellow
$response = Send-Cmd "CREATE_TOPIC orders 3"
Write-Host "Response: $response" -ForegroundColor Cyan

# Test 2: Produce Message
Write-Host "`n[TEST 2] Producing message..." -ForegroundColor Yellow
$response = Send-Cmd "PRODUCE orders key1 Hello"
Write-Host "Response: $response" -ForegroundColor Cyan

# Test 3: Get Metadata
Write-Host "`n[TEST 3] Getting metadata..." -ForegroundColor Yellow
$response = Send-Cmd "METADATA orders"
Write-Host "Response: $response" -ForegroundColor Cyan

# Test 4: Consume Message
Write-Host "`n[TEST 4] Consuming message from partition 0..." -ForegroundColor Yellow
$response = Send-Cmd "CONSUME orders 0 0"
Write-Host "Response: $response" -ForegroundColor Cyan

Write-Host "`n=== Test Complete ===" -ForegroundColor Green
'@ | Out-File -FilePath test_now.ps1 -Encoding UTF8