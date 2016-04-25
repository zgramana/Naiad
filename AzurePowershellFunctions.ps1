Add-Type @'
public class NaiadAccounts
{
    public System.Management.Automation.PSCredential credential;
	public System.String resourceGroupName;
	public System.String storageAccountName;
	public System.String storageAccountKey;
}
'@

Add-Type @'
public class NaiadVM
{
    public System.String ipAddress;
	public System.Int32 frontendPort;
	public System.Int32 index;
}
'@

function Naiad-ConfigureAccounts
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$resourceGroupName
    )
    PROCESS {
        
        $result = New-Object NaiadAccounts;

        $result.credential = Get-Credential
        $result.resourceGroupName = $resourceGroupName
        $storageAccount = Get-AzureRmStorageAccount -ResourceGroupName $resourceGroupName | Sort-Object | Select-Object -First 1
		$result.storageAccountName = $storageAccount.StorageAccountName
		$result.storageAccountKey = (Get-AzureRmStorageAccountKey -ResourceGroupName $resourceGroupName -StorageAccountName $storageAccount.StorageAccountName).key1

        $result
    }
}

function Naiad-PrepareContainer
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
    )
    PROCESS {
        $storageContext = New-AzureStorageContext -StorageAccountName $accountInformation.storageAccountName -StorageAccountKey $accountInformation.storageAccountKey
        New-AzureStorageContainer -Context $storageContext -Name "executables" -Permission Blob
    }
}

function Naiad-PrepareJob
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$pathToExecutable,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
    )
    PROCESS {

        $guid = [guid]::NewGuid()

        $package = "C:\temp\" + $guid + ".zip"

        $loadresult = [Reflection.Assembly]::LoadWithPartialName("System.IO.Compression.FileSystem")
        $compressResult = [System.IO.Compression.ZipFile]::CreateFromDirectory($pathToExecutable, $package)

        # push the zipped file to Azure blob store.
        $storageContext = New-AzureStorageContext -StorageAccountName $accountInformation.storageAccountName -StorageAccountKey $accountInformation.storageAccountKey
        $container = Get-AzureStorageContainer -Context $storageContext -Name "executables"
        $blob = Set-AzureStorageBlobContent -Context $storageContext -Container $container.Name -Blob $guid -File $package

        $guid
    }
}

#deploys a Naiad job from a guid and account information
function Naiad-DeployJob
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [Guid]$guid,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
    )
    PROCESS {
		$rg = $accountInformation.resourceGroupName;

		$dnsName = (Get-AzureRmPublicIpAddress -ResourceGroupName $rg).DnsSettings.Fqdn;
        $portRules = (Get-AzureRmLoadBalancer -ResourceGroupName $rg).InboundNatRules | where -Property BackendPort -EQ 5986

        # find the zipped file in Azure blob store.
        $storageContext = New-AzureStorageContext -StorageAccountName $accountInformation.storageAccountName -StorageAccountKey $accountInformation.storageAccountKey
        $container = Get-AzureStorageContainer -Context $storageContext -Name "executables"
        $blob = Get-AzureStorageBlob -Context $storageContext -Container $container.Name -Blob $guid
        $bloburi = $blob.ICloudBlob.Uri

        # for each VM, if the zip file isn't already there, download it
        $deployJobs = @()
        $portRules | ForEach-Object {
			Invoke-Command -ScriptBlock { 
				$bloburi = $args[0]
	            $guid = $args[1]

				if (!(Test-Path "C:\temp"))
				{
					mkdir "C:\temp"
				}

				if (Test-Path "C:\temp\$guid")
				{
					# already uploaded, 
				}
				else
				{
					wget $args[0] -UseBasicParsing -OutFile "C:\temp\$guid.zip"
					$a = [Reflection.Assembly]::LoadWithPartialName("System.IO.Compression.FileSystem")
					[System.IO.Compression.ZipFile]::ExtractToDirectory("C:\temp\$guid.zip", "C:\temp\$guid\")
				}
				"done";
			} -ComputerName $dnsName -Port $_.FrontendPort -UseSSL -AsJob -Credential $accountInformation.credential -ArgumentList $bloburi, $guid
        }
    }
}

# update the powershell quotas on each VM to be allowed to use more than 1GB: otherwise powershell will silently kill any process that
# exceeds this amount of memory
function Naiad-PrepareVMs
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
        
    )
    PROCESS {
		$rg = $accountInformation.resourceGroupName;

		$dnsName = (Get-AzureRmPublicIpAddress -ResourceGroupName $rg).DnsSettings.Fqdn;
        $portRules = (Get-AzureRmLoadBalancer -ResourceGroupName $rg).InboundNatRules | where -Property BackendPort -EQ 5986

        $jobs = $portRules | ForEach-Object {
			Invoke-Command -AsJob -ComputerName $dnsName -Port $_.FrontendPort -ScriptBlock {
				netsh firewall add portopening TCP 2101 "Naiad"

		        Set-Item WSMan:\localhost\Shell\MaxMemoryPerShellMB 16000 -Force
			    Set-Item WSMan:\localhost\Plugin\microsoft.powershell\Quotas\MaxMemoryPerShellMB 16000
    
			    Restart-Service winrm
			} -Credential $accountInformation.credential -UseSSL -ArgumentList $processname
		}

        Wait-Job $jobs
		Receive-Job $jobs
    }
}

# executes a Naiad job from a guid, account information, and executable string
function Naiad-ExecuteJob
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [Guid]$guid,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation,
        [parameter(Mandatory=$true)]
        [string]$executableName,
        [parameter(Mandatory=$true)]
        [string]$additionalArguments
    )
    PROCESS {
		$rg = $accountInformation.resourceGroupName;

		$dnsName = (Get-AzureRmPublicIpAddress -ResourceGroupName $rg).DnsSettings.Fqdn;
        $portRules = (Get-AzureRmLoadBalancer -ResourceGroupName $rg).InboundNatRules | where -Property BackendPort -EQ 5986
		$vmssName = (Get-AzureRmVmss -ResourceGroupName $rg).Name
		$ipconfigs = Get-AzureRmNetworkInterface -ResourceGroupName naiad -VirtualMachineScaleSetName $vmssName | select -ExpandProperty ipconfigurations

		$rulesToIp = @{}
		$ipconfigs | ForEach-Object {
			$config = $_
			$config.LoadBalancerInboundNatRules | ForEach-Object {
				$rulesToIp[$_.Id] = $config.PrivateIpAddress
			}
		}

        # make a mapping that assigns each VM an integer
        $i = 0
        $mapping = @{}
        $mapping = $portRules | ForEach-Object {
			$rule = $_;
			$vm = New-Object NaiadVM
			$vm.frontendPort = $rule.FrontendPort
			$vm.ipAddress = $rulesToIp[$rule.Id]
			$vm.index = $i++
			$vm
		}

        # make the connection string that the executables will need to talk to blob storage
        $connectionString = "DefaultEndpointsProtocol=http;AccountName=" + $accountInformation.storageAccountName + ";AccountKey=" + $accountInformation.storageAccountKey + ";"

        $mapping | ForEach-Object {
            Invoke-Command -ScriptBlock { 
        
                $guid = $args[1]
                $mapping = $args[2]
				$procId = $args[3]
                $connectionString = $args[4]
                $executableName = $args[5]
                $additionalArguments = $args[6]

                $argList = $additionalArguments.Split()
    
                # augment the arguments with default Naiad args: you may want to edit this. It assumes 7 threads per process
                $argList += @("-n", $mapping.Count, "-t", "1", "-p", $procID, "--addsetting",  "Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString", $connectionString, "-h")
                #$argList += @("-n", $mapping.Count, "-t", "7", "--addsetting",  "Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString", $connectionString, "--inlineserializer", "-p", $procID, "-h")
                # supply the listener ports for all the processes in the job
                (0..($mapping.Count-1)) | ForEach-Object { $argList += $mapping[$_].ipAddress + ":2101 " }


                cd c:\temp\$guid\

                echo "C:\temp\$guid\$executableName" $argList
                & "C:\temp\$guid\$executableName" $argList 2>&1 | %{ "$_".TrimEnd() }
                #& "C:\temp\$guid\$executableName" $argList 2>&1 | %{ "$_".TrimEnd() } > C:\temp\$guid\output.txt
            } -AsJob -ComputerName $dnsName -Port $_.frontEndPort -Credential $accountInformation.credential -UseSSL -ArgumentList $bloburi,$guid,$mapping,$_.index,$connectionString,$executableName,$additionalArguments
        }
    }
}

# stop all the processes with a given name on the VMs
function Naiad-StopAzureProcess
{
    [CmdletBinding()]
    param (

        [parameter(Mandatory=$true)]
        [string]$processname,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
        
    )
    PROCESS {
		$rg = $accountInformation.resourceGroupName;

		$dnsName = (Get-AzureRmPublicIpAddress -ResourceGroupName $rg).DnsSettings.Fqdn;
        $portRules = (Get-AzureRmLoadBalancer -ResourceGroupName $rg).InboundNatRules | where -Property BackendPort -EQ 5986

        $jobs = $portRules | ForEach-Object { Invoke-Command -AsJob -ComputerName $dnsName -Port $_.FrontendPort -ScriptBlock { Stop-Process -Name $args[0] } -Credential $accountInformation.credential -UseSSL -ArgumentList $processname }

        Wait-Job $jobs
    }
}

# get the memory being used by each process
function Naiad-GetAzureProcessMemory
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$processname,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
	)
    PROCESS {
		$rg = $accountInformation.resourceGroupName;

		$dnsName = (Get-AzureRmPublicIpAddress -ResourceGroupName $rg).DnsSettings.Fqdn;
        $portRules = (Get-AzureRmLoadBalancer -ResourceGroupName $rg).InboundNatRules | where -Property BackendPort -EQ 5986

        $jobs = $portRules | ForEach-Object { Invoke-Command -AsJob -ComputerName $dnsName -Port $_.FrontendPort -ScriptBlock { (Get-Process -Name $args[0]).PagedMemorySize64 } -Credential $accountInformation.credential -UseSSL -ArgumentList $processname }

        Wait-Job $jobs
    }
}

function Naiad-ConnectRemoteDesktop 
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation,
        [parameter(Mandatory=$true)]
        [string]$hostname
    )
    PROCESS {
        Get-AzureRemoteDesktopFile -ServiceName $accountInformation.cloudServiceName -Name $hostname -Launch
    }
}
