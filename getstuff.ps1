# the downloader object
$client = new-object System.Net.WebClient
# path to current directory
$path = (Get-Item -Path ".\" -Verbose).FullName

# downloading function
function Download ($link,$file,$ext="exe")
{
    echo "Getting ${link}${file}.${ext}"
    $client.DownloadFile("${link}/${file}.${ext}","${path}\${file}.${ext}")
}

# downloading putty components
$addr = "http://the.earth.li/~sgtatham/putty/latest/x86/"
$execs = @("pscp","plink","putty","puttygen","pageant")

foreach ($exec in $execs) {
    Download -link $addr -file $exec
}

# download python
$python_addr = "https://www.python.org/ftp/python/3.4.1/"
$python_exec = "python-3.4.1"
Download -link $python_addr -file $python_exec -ext "msi"

