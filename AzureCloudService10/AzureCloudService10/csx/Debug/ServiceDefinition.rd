﻿<?xml version="1.0" encoding="utf-8"?>
<serviceModel xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" name="AzureCloudService10" generation="1" functional="0" release="0" Id="317474fe-3b9e-4497-8f36-4f8df82dd488" dslVersion="1.2.0.0" xmlns="http://schemas.microsoft.com/dsltools/RDSM">
  <groups>
    <group name="AzureCloudService10Group" generation="1" functional="0" release="0">
      <componentports>
        <inPort name="WebRole1:Endpoint1" protocol="http">
          <inToChannel>
            <lBChannelMoniker name="/AzureCloudService10/AzureCloudService10Group/LB:WebRole1:Endpoint1" />
          </inToChannel>
        </inPort>
      </componentports>
      <settings>
        <aCS name="WebRole1:APPINSIGHTS_INSTRUMENTATIONKEY" defaultValue="">
          <maps>
            <mapMoniker name="/AzureCloudService10/AzureCloudService10Group/MapWebRole1:APPINSIGHTS_INSTRUMENTATIONKEY" />
          </maps>
        </aCS>
        <aCS name="WebRole1:Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" defaultValue="">
          <maps>
            <mapMoniker name="/AzureCloudService10/AzureCloudService10Group/MapWebRole1:Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" />
          </maps>
        </aCS>
        <aCS name="WebRole1Instances" defaultValue="[1,1,1]">
          <maps>
            <mapMoniker name="/AzureCloudService10/AzureCloudService10Group/MapWebRole1Instances" />
          </maps>
        </aCS>
        <aCS name="WorkerRole1:APPINSIGHTS_INSTRUMENTATIONKEY" defaultValue="">
          <maps>
            <mapMoniker name="/AzureCloudService10/AzureCloudService10Group/MapWorkerRole1:APPINSIGHTS_INSTRUMENTATIONKEY" />
          </maps>
        </aCS>
        <aCS name="WorkerRole1:Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" defaultValue="">
          <maps>
            <mapMoniker name="/AzureCloudService10/AzureCloudService10Group/MapWorkerRole1:Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" />
          </maps>
        </aCS>
        <aCS name="WorkerRole1Instances" defaultValue="[1,1,1]">
          <maps>
            <mapMoniker name="/AzureCloudService10/AzureCloudService10Group/MapWorkerRole1Instances" />
          </maps>
        </aCS>
      </settings>
      <channels>
        <lBChannel name="LB:WebRole1:Endpoint1">
          <toPorts>
            <inPortMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1/Endpoint1" />
          </toPorts>
        </lBChannel>
      </channels>
      <maps>
        <map name="MapWebRole1:APPINSIGHTS_INSTRUMENTATIONKEY" kind="Identity">
          <setting>
            <aCSMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1/APPINSIGHTS_INSTRUMENTATIONKEY" />
          </setting>
        </map>
        <map name="MapWebRole1:Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" kind="Identity">
          <setting>
            <aCSMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1/Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" />
          </setting>
        </map>
        <map name="MapWebRole1Instances" kind="Identity">
          <setting>
            <sCSPolicyIDMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1Instances" />
          </setting>
        </map>
        <map name="MapWorkerRole1:APPINSIGHTS_INSTRUMENTATIONKEY" kind="Identity">
          <setting>
            <aCSMoniker name="/AzureCloudService10/AzureCloudService10Group/WorkerRole1/APPINSIGHTS_INSTRUMENTATIONKEY" />
          </setting>
        </map>
        <map name="MapWorkerRole1:Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" kind="Identity">
          <setting>
            <aCSMoniker name="/AzureCloudService10/AzureCloudService10Group/WorkerRole1/Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" />
          </setting>
        </map>
        <map name="MapWorkerRole1Instances" kind="Identity">
          <setting>
            <sCSPolicyIDMoniker name="/AzureCloudService10/AzureCloudService10Group/WorkerRole1Instances" />
          </setting>
        </map>
      </maps>
      <components>
        <groupHascomponents>
          <role name="WebRole1" generation="1" functional="0" release="0" software="C:\Users\Kensei\Documents\Visual Studio 2015\Projects\AzureCloudService10\AzureCloudService10\csx\Debug\roles\WebRole1" entryPoint="base\x64\WaHostBootstrapper.exe" parameters="base\x64\WaIISHost.exe " memIndex="-1" hostingEnvironment="frontendadmin" hostingEnvironmentVersion="2">
            <componentports>
              <inPort name="Endpoint1" protocol="http" portRanges="80" />
            </componentports>
            <settings>
              <aCS name="APPINSIGHTS_INSTRUMENTATIONKEY" defaultValue="" />
              <aCS name="Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" defaultValue="" />
              <aCS name="__ModelData" defaultValue="&lt;m role=&quot;WebRole1&quot; xmlns=&quot;urn:azure:m:v1&quot;&gt;&lt;r name=&quot;WebRole1&quot;&gt;&lt;e name=&quot;Endpoint1&quot; /&gt;&lt;/r&gt;&lt;r name=&quot;WorkerRole1&quot; /&gt;&lt;/m&gt;" />
            </settings>
            <resourcereferences>
              <resourceReference name="DiagnosticStore" defaultAmount="[4096,4096,4096]" defaultSticky="true" kind="Directory" />
              <resourceReference name="EventStore" defaultAmount="[1000,1000,1000]" defaultSticky="false" kind="LogStore" />
            </resourcereferences>
          </role>
          <sCSPolicy>
            <sCSPolicyIDMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1Instances" />
            <sCSPolicyUpdateDomainMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1UpgradeDomains" />
            <sCSPolicyFaultDomainMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1FaultDomains" />
          </sCSPolicy>
        </groupHascomponents>
        <groupHascomponents>
          <role name="WorkerRole1" generation="1" functional="0" release="0" software="C:\Users\Kensei\Documents\Visual Studio 2015\Projects\AzureCloudService10\AzureCloudService10\csx\Debug\roles\WorkerRole1" entryPoint="base\x64\WaHostBootstrapper.exe" parameters="base\x64\WaWorkerHost.exe " memIndex="-1" hostingEnvironment="consoleroleadmin" hostingEnvironmentVersion="2">
            <settings>
              <aCS name="APPINSIGHTS_INSTRUMENTATIONKEY" defaultValue="" />
              <aCS name="Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString" defaultValue="" />
              <aCS name="__ModelData" defaultValue="&lt;m role=&quot;WorkerRole1&quot; xmlns=&quot;urn:azure:m:v1&quot;&gt;&lt;r name=&quot;WebRole1&quot;&gt;&lt;e name=&quot;Endpoint1&quot; /&gt;&lt;/r&gt;&lt;r name=&quot;WorkerRole1&quot; /&gt;&lt;/m&gt;" />
            </settings>
            <resourcereferences>
              <resourceReference name="DiagnosticStore" defaultAmount="[4096,4096,4096]" defaultSticky="true" kind="Directory" />
              <resourceReference name="EventStore" defaultAmount="[1000,1000,1000]" defaultSticky="false" kind="LogStore" />
            </resourcereferences>
          </role>
          <sCSPolicy>
            <sCSPolicyIDMoniker name="/AzureCloudService10/AzureCloudService10Group/WorkerRole1Instances" />
            <sCSPolicyUpdateDomainMoniker name="/AzureCloudService10/AzureCloudService10Group/WorkerRole1UpgradeDomains" />
            <sCSPolicyFaultDomainMoniker name="/AzureCloudService10/AzureCloudService10Group/WorkerRole1FaultDomains" />
          </sCSPolicy>
        </groupHascomponents>
      </components>
      <sCSPolicy>
        <sCSPolicyUpdateDomain name="WebRole1UpgradeDomains" defaultPolicy="[5,5,5]" />
        <sCSPolicyUpdateDomain name="WorkerRole1UpgradeDomains" defaultPolicy="[5,5,5]" />
        <sCSPolicyFaultDomain name="WebRole1FaultDomains" defaultPolicy="[2,2,2]" />
        <sCSPolicyFaultDomain name="WorkerRole1FaultDomains" defaultPolicy="[2,2,2]" />
        <sCSPolicyID name="WebRole1Instances" defaultPolicy="[1,1,1]" />
        <sCSPolicyID name="WorkerRole1Instances" defaultPolicy="[1,1,1]" />
      </sCSPolicy>
    </group>
  </groups>
  <implements>
    <implementation Id="36583068-8b37-4806-a96a-a24cca98dd7f" ref="Microsoft.RedDog.Contract\ServiceContract\AzureCloudService10Contract@ServiceDefinition">
      <interfacereferences>
        <interfaceReference Id="73757f6d-b42c-431d-ac98-58d343aa88e1" ref="Microsoft.RedDog.Contract\Interface\WebRole1:Endpoint1@ServiceDefinition">
          <inPort>
            <inPortMoniker name="/AzureCloudService10/AzureCloudService10Group/WebRole1:Endpoint1" />
          </inPort>
        </interfaceReference>
      </interfacereferences>
    </implementation>
  </implements>
</serviceModel>