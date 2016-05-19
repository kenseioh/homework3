using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Configuration;
using System.IO;
using System.Xml;
using HtmlAgilityPack;
using ClassLibrary1;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text.RegularExpressions;
using System.Text;

namespace WorkerRole1
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue stopqueue = queueClient.GetQueueReference("stopqueue");


            CloudQueue xmlqueue = queueClient.GetQueueReference("xmlstorage");
            CloudQueue urlqueue = queueClient.GetQueueReference("urlstorage");
            xmlqueue.CreateIfNotExists();
            urlqueue.CreateIfNotExists();
            HtmlWeb web = new HtmlWeb();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable tables = tableClient.GetTableReference("table");
            tables.CreateIfNotExists();
            CloudTable dashboardTable = tableClient.GetTableReference("dashboardtable");
            dashboardTable.CreateIfNotExists();


            Queue<string> dashboardWord = new Queue<string>();
            string cnn = "";
            //int tablecount = 0;
            int queuecount = 0;
            var disallow = new List<string> { "/editionssi", "/ads", "/aol", "/audio", "/beta", "/browsers", "/cl", "/cnews", "/cnn_adspaces", "/cnnbeta", "/cnnintl_adspaces",
                "/development", "/help/cnnx.html", "/NewsPass", "/NOKIA", "/partners", "/pipeline", "/pointroll", "/POLLSERVER", "/pr/", "/PV", "/quickcast", "/AAMSZ=160x600",
                "/AAMSZ=300x250", "/AAMSZ=728x90", "/account", "/accounts", "/activities", "/add_to_blacklist", "/login", "/long_poll", "/new_writer" ,"/newsletter_subscriptions",
                "/pages", "/signup", "/users/rankings", "/Quickcast", "/QUICKNEWS", "/test", "/virtual", "/WEB-INF", "/web.projects", "/search"};


            string stringMessage;
            var client = new WebClient();
            Stream stream = client.OpenRead("http://www.cnn.com/robots.txt");
            Stream file = client.OpenRead("http://bleacherreport.com/robots.txt");
            HashSet<String> duplicates = new HashSet<string>();
            HashSet<String> tableDuplicates = new HashSet<string>();
            CheckEntity countpage = new CheckEntity();
            using (StreamReader sr = new StreamReader(stream))
            {
                using (StreamReader srr = new StreamReader(file))
                {
                    string line;
                    List<string> lines = new List<string>();
                    // Read and display lines from the file until the end of
                    // the file is reached.
                    while ((line = sr.ReadLine()) != null && line.Contains(".xml") )//&& stopqueue.Exists())
                    {
                        string[] word = line.Split(' ');
                        CloudQueueMessage message = new CloudQueueMessage(word[1]);
                        xmlqueue.AddMessageAsync(message);

                    }
                    while ((line = srr.ReadLine()) != null && line.Contains(".xml") && stopqueue.Exists())
                    {
                        string[] word = line.Split(' ');
                        CloudQueueMessage message = new CloudQueueMessage(word[1]);
                        if (message.AsString.Contains("nba"))
                            xmlqueue.AddMessageAsync(message);

                    }
                }
            }
            while (true && stopqueue.Exists())
            {
                countpage.ram = getAvailableRAM();
                countpage.cpu = getCurrentCpuUsage();
                TableOperation insertOperations = TableOperation.InsertOrReplace(countpage);
                CloudQueueMessage getMessage = xmlqueue.GetMessage();

                while (getMessage != null && stopqueue.Exists())
                {

                    xmlqueue.DeleteMessage(getMessage);
                    stringMessage = getMessage.AsString;
                    string sitemap = stringMessage;

                    XmlDocument xDoc = new XmlDocument();
                    Regex regex = new Regex(@"-(\d{4})-(\d{2}).xml");
                    //load up the xml from the location
                    xDoc.Load(sitemap);

                    // cycle through each child noed
                    foreach (XmlNode node in xDoc.DocumentElement.ChildNodes)
                    {
                        // first node is the url ... have to go to nexted loc node
                        foreach (XmlNode locNode in node)
                        {
                            // thereare a couple child nodes here so only take data from node named loc
                            if (locNode.Name == "loc")
                            {
                                // get the content of the loc node
                                string loc = locNode.InnerText;
                                if (loc.EndsWith(".xml"))
                                {
                                    // write it to the console so you can see its working
                                    Console.WriteLine(loc + Environment.NewLine);
                                    CloudQueueMessage message = new CloudQueueMessage(loc);

                                    if (!duplicates.Contains(message.AsString))
                                    {
                                        duplicates.Add(message.AsString);
                                        Match match = regex.Match(loc);
                                        if (match.Success)
                                        {
                                            int year = Int32.Parse(match.Groups[1].Value);
                                            int month = Int32.Parse(match.Groups[2].Value);
                                            if (month >= 3 && year >= 2016)
                                            {
                                                if (xmlqueue.ApproximateMessageCount < 3)
                                                {
                                                if (message.AsString.Contains(@"http://bleacherreport.com/sitemap/nba.xml") || message.AsString.Contains("cnn.com"))
                                                {
                                                    xmlqueue.AddMessageAsync(message);
                                                }
                                                }
                                            }
                                        }
                                    }
                                }
                                else //if (loc.EndsWith(".html") || loc.EndsWith(".htm"))
                                {
                                    CloudQueueMessage message = new CloudQueueMessage(loc);
                                    if (!duplicates.Contains(message.AsString))
                                    {
                                        if (message.AsString.Contains("cnn.com") || message.AsString.Contains("bleacherreport"))
                                        {
                                            duplicates.Add(message.AsString);
                                            urlqueue.AddMessageAsync(message);
                                            queuecount++;
                                            //countpage.queueSize = queuecount;
                                            insertOperations = TableOperation.InsertOrReplace(countpage);
                                            dashboardTable.Execute(insertOperations);
                                        }

                                    }
                                }
                            }
                        }

                    }
                    getMessage = xmlqueue.GetMessage();
                }
                CloudQueueMessage getMessage2 = urlqueue.GetMessage();
                while (getMessage2 != null && stopqueue.Exists())
                {
                    try
                    {
                        foreach (string disallows in disallow)
                        {
                            if (getMessage2.AsString.Contains(disallows))
                            {
                                if (getMessage2 != null)
                                {
                                    getMessage2 = urlqueue.GetMessage();
                                }
                            }
                        }
                        if (getMessage2.AsString.StartsWith("/"))
                        {
                            cnn = "http://cnn.com" + getMessage2.AsString;
                            getMessage2 = new CloudQueueMessage(cnn);
                        }
                        else if (getMessage2.AsString.StartsWith("//"))
                        {
                            cnn = "http:" + getMessage2.AsString;
                            getMessage2 = new CloudQueueMessage(cnn);
                        }
                        HtmlDocument document = web.Load(getMessage2.AsString);
                        HtmlNode[] nodes = document.DocumentNode.SelectNodes("//link[@href]").ToArray();
                        urlqueue.DeleteMessage(getMessage2);
                        queuecount--;
                        //countpage.queueSize = queuecount;
                        insertOperations = TableOperation.InsertOrReplace(countpage);
                        dashboardTable.Execute(insertOperations);

                        var titleNode = document.DocumentNode.SelectSingleNode("//title");
                        string title = titleNode.InnerText;

                        var dateNode = document.DocumentNode.SelectSingleNode("//meta[@name='pubdate']");
                        string date = "";
                        if (dateNode != null)
                        {
                            date = dateNode.Attributes["content"].Value;
                        }
                        foreach (HtmlNode link in nodes)
                        {
                            HtmlAttribute att = link.Attributes["href"];
                            if (att.Value.Contains(@"http://www.cnn.com") || att.Value.Contains(@"http://bleacherreport.com"))
                            {
                                //if (!duplicates.Contains(att.Value.ToString()))
                                //{

                                CloudQueueMessage message2 = new CloudQueueMessage(att.Value.ToString());
                                duplicates.Add(att.Value.ToString());
                                urlqueue.AddMessageAsync(message2);
                                queuecount++;
                                //countpage.queueSize = queuecount;
                                insertOperations = TableOperation.InsertOrReplace(countpage);
                                dashboardTable.Execute(insertOperations);

                                //}
                            }
                        }
                        if (!tableDuplicates.Contains(getMessage2.AsString))
                        {
                            tableDuplicates.Add(getMessage2.AsString);
                            WebEntity webpage = new WebEntity(CreateMD5(getMessage2.AsString));
                            webpage.date = date;
                            webpage.title = title;
                            webpage.url = getMessage2.AsString;
                            insertOperations = TableOperation.Insert(webpage);
                            tables.Execute(insertOperations);
                            
                            countpage.indexSize = countpage.indexSize + 1;
                            insertOperations = TableOperation.InsertOrReplace(countpage);
                            dashboardTable.Execute(insertOperations);
                            dashboardWord.Enqueue(getMessage2.AsString);
                            if (dashboardWord.Count() == 10)
                            {
                                string dashboardString = "";
                                foreach (string word in dashboardWord.ToArray())
                                {
                                    dashboardString = word + ", " + dashboardString;

                                }

                                countpage.url = dashboardString;
                                
                                countpage.queueSize = queuecount;
                                countpage.ram = getAvailableRAM();
                                countpage.cpu = getCurrentCpuUsage();
                                insertOperations = TableOperation.InsertOrReplace(countpage);
                                dashboardTable.Execute(insertOperations);
                                dashboardWord.Dequeue();

                            }

                        }
                    }
                    catch { getMessage2 = urlqueue.GetMessage(); }

                }
                Trace.TraceInformation("WorkerRole1 is running");
                try
                {
                    this.RunAsync(this.cancellationTokenSource.Token).Wait();
                }
                finally
                {
                    this.runCompleteEvent.Set();
                }


            }
        }

        PerformanceCounter cpuCounter;
        PerformanceCounter ramCounter;

        public object CloudConfigurationManager { get; private set; }


        public string getCurrentCpuUsage()
        {
            List<string> testCPU = new List<string>();
            cpuCounter = new PerformanceCounter();
            cpuCounter.CategoryName = "Processor";
            cpuCounter.CounterName = "% Processor Time";
            cpuCounter.InstanceName = "_Total";
            cpuCounter.NextValue();
            System.Threading.Thread.Sleep(1000);
            return (cpuCounter.NextValue() + "%");

        }


        public string getAvailableRAM()
        {
            List<string> testRAM = new List<string>();
            ramCounter = new PerformanceCounter("Memory", "Available MBytes");
            return (ramCounter.NextValue() + "MB");

        }

        private static string CreateMD5(string input)
        {
            // Use input string to calculate MD5 hash
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    sb.Append(hashBytes[i].ToString("X2"));
                }
                return sb.ToString();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("WorkerRole1 has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WorkerRole1 is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("WorkerRole1 has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(1000);
            }
        }
    }
}
