using ClassLibrary1;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Web;
using System.Web.Script.Serialization;
using System.Web.Script.Services;
using System.Web.Services;
using System.Xml;

namespace WebRole1
{
    /// <summary>
    /// Summary description for Admin
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    [System.Web.Script.Services.ScriptService]
    public class Admin : System.Web.Services.WebService
    {
        string filename = System.IO.Path.GetTempPath() + "\\robots.txt";
        string filename2 = System.IO.Path.GetTempPath() + "\\bleacher-robots.txt";

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
            public string urlTable(string url)
        {
            List<string> test = new List<string>();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            CloudTable table = tableClient.GetTableReference("urlstorage");
            var query = from entity in table.CreateQuery<WebEntity>()
                        where entity.url == url
                        select entity.title;
            return new JavaScriptSerializer().Serialize(query.ToList<string>());

        }


        public object CloudConfigurationManager { get; private set; }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string getCurrentCpuUsage()
        {
            List<string> listWord = new List<string>();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable dashboardTable = tableClient.GetTableReference("dashBoardTable");
            TableQuery<CheckEntity> query = new TableQuery<CheckEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "partition"));
            foreach (CheckEntity entity in dashboardTable.ExecuteQuery(query))
            {
                listWord.Add(entity.cpu);
            }

            return new JavaScriptSerializer().Serialize(listWord);
        }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string getAvailableRAM()
        {
            List<string> listWord = new List<string>();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable dashboardTable = tableClient.GetTableReference("dashBoardTable");
            TableQuery<CheckEntity> query = new TableQuery<CheckEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "partition"));
            foreach (CheckEntity entity in dashboardTable.ExecuteQuery(query))
            {
                listWord.Add(entity.ram);
            }

            return new JavaScriptSerializer().Serialize(listWord);
        }


        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string StartCrawling()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue stopqueue = queueClient.GetQueueReference("stopqueue");
            stopqueue.CreateIfNotExists();
            return "done";
        }
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string StopCrawling()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue stopqueue = queueClient.GetQueueReference("stopqueue");
            stopqueue.DeleteIfExists();
            return "done";
        }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string ClearIndex()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudQueue xmlqueue = queueClient.GetQueueReference("xmlstorage");
            CloudQueue urlqueue = queueClient.GetQueueReference("urlstorage");
            CloudTable tables = tableClient.GetTableReference("table");
            CloudTable dashboardTable = tableClient.GetTableReference("dashboardtable");
            xmlqueue.DeleteIfExists();
            urlqueue.DeleteIfExists();
            tables.DeleteIfExists();
            dashboardTable.DeleteIfExists();

            return "";
        }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string GetPageTitle()
        {
            List<string> listWord = new List<string>();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable dashboardTable = tableClient.GetTableReference("dashBoardTable");
            TableQuery<CheckEntity> query = new TableQuery<CheckEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "partition"));
            foreach (CheckEntity entity in dashboardTable.ExecuteQuery(query))
            {
                listWord.Add(entity.url);                    
            }
            
            return new JavaScriptSerializer().Serialize(listWord);
            
        }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string GetQueueSize()
        {
            List<string> listWord = new List<string>();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            var temp = queueClient.GetQueueReference("urlstorage");
            temp.FetchAttributes();
            listWord.Add(temp.ApproximateMessageCount.ToString());
            return new JavaScriptSerializer().Serialize(listWord);/*
             temp.ApproximateMessageCount.ToString();
            CloudTable dashboardTable = tableClient.GetTableReference("dashBoardTable");

            TableQuery<CheckEntity> query = new TableQuery<CheckEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "partition"));
            foreach (CheckEntity entity in dashboardTable.ExecuteQuery(query))
            {
                listWord.Add(entity.queueSize);
            }

            return new JavaScriptSerializer().Serialize(listWord);
            */
        }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string GetIndexSize()
        {
            List<int> listWord = new List<int>();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();;
            CloudTable dashboardTable = tableClient.GetTableReference("dashBoardTable");
            TableQuery<CheckEntity> query = new TableQuery<CheckEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "partition"));
            foreach (CheckEntity entity in dashboardTable.ExecuteQuery(query))
            {
                listWord.Add(entity.indexSize);
            }

            return new JavaScriptSerializer().Serialize(listWord);

        }
    }
}
