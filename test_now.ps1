# test_now.ps1
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
        $response = $reader.ReadLine()
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
# Test 2: Produce message
Write-Host "`n[TEST 2] Producing message..." -ForegroundColor Yellow
$produceResponse = Send-Cmd "PRODUCE orders key1 Hello"
Write-Host "Response: $produceResponse" -ForegroundColor Cyan

# Parse the JSON response to get partition
try {
    $produceObj = $produceResponse | ConvertFrom-Json
    $partition = $produceObj.partition
    $offset = 0  # Always start from offset 0 for first consume
    Write-Host "Message produced to partition: $partition, offset: $offset" -ForegroundColor Green
} catch {
    Write-Host "Failed to parse produce response: $produceResponse" -ForegroundColor Red
    $partition = 0  # Default fallback
    $offset = 0
}
Write-Host "Response: $response" -ForegroundColor Cyan
# Test 3: Get Metadata
Write-Host "`n[TEST 3] Getting metadata..." -ForegroundColor Yellow
$response = Send-Cmd "METADATA orders"
Write-Host "Response: $response" -ForegroundColor Cyan
# Test 4: Consume Message
Write-Host "`n[TEST 4] Consuming message from partition $partition..." -ForegroundColor Yellow
$response = Send-Cmd "CONSUME orders $partition $offset"
Write-Host "Response: $response" -ForegroundColor Cyan
Write-Host "`n=== Test Complete ===" -ForegroundColor Green
