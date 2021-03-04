using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace dunzoCallbackProcessor
{
    public class Functions
    {
        // This function will get triggered/executed when a new message is written 
        // on an Azure Queue called queue.
        public static void ProcessQueueMessage([QueueTrigger("dunzocallbackq")] string message, TextWriter log)
        {
            string[] words = message.Split('_');
            log.WriteLine(message);
            string blobFileName;
            int merchantId = 0;
            Console.WriteLine(message);
            if (words[0] == "dunzocallback")
            {
                try
                {
                    merchantId = int.Parse(words[1]);
                    blobFileName = message;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Could not get merchantid ");
                    return;
                }
                pydukaanProcessNewDUnzoCallback(merchantId, blobFileName);
            }
            else
            {
                Console.WriteLine("Unsupported Delivery Partner");
            }
        }

        public static void pydukaanProcessNewDUnzoCallback(int merchantId, string blobFileName)
        {
            // Get Container Name
            string containerName = pydukaanGetContainerName(merchantId);

            // Create Container 
            CloudBlobContainer blobContainer = pydukaanBlobCreateContainer(containerName);
            // Download Filename in memory stream
            MemoryStream dataInms = pydukaanDownloadJsonFile(blobFileName, blobContainer);
            byte[] buffer = new byte[(int)dataInms.Length];
            dataInms.Read(buffer, 0, (int)dataInms.Length);
            ProcessBlobFileContent(buffer, blobFileName);
                       
        }
        public static void ProcessBlobFileContent(byte[] buffer,string blobFileName)
        {
            try
            {
                string blobString = System.Text.Encoding.UTF8.GetString(buffer);
                if (blobString.Contains("queued") ||  blobString.Contains("runner_cancelled"))
                {
                    queuedRunner_cancelled respJson = JsonConvert.DeserializeObject<queuedRunner_cancelled>(blobString);
                    Console.WriteLine(" Task Id is:  " + respJson.task_id);
                    Console.WriteLine(" State is:  " + respJson.state);
                    Console.WriteLine(" Pickup Eta is:  " + respJson.eta.pickup);
                    Console.WriteLine(" Drop Eta is:  " + respJson.eta.dropoff);
                    pydukaanInsertCallBackStatusIntoSnapShotTable(respJson.task_id);
                    pydukaanIUpdateCallBackStatusForQueued(respJson.task_id,respJson.state,respJson.eta.pickup,
                        respJson.eta.dropoff,blobFileName);

                }
                else if (blobString.Contains("runner_accepted") || blobString.Contains("reached_for_pickup"))
                {
                    runner_acceptedReached_for_pickup respJson = JsonConvert.DeserializeObject<runner_acceptedReached_for_pickup>(blobString);
                    Console.WriteLine(" Task Id is:  " + respJson.task_id);
                    Console.WriteLine(" State is:  " + respJson.state);
                    Console.WriteLine(" Pickup Eta is:  " + respJson.eta.pickup);
                    Console.WriteLine(" Drop Eta s:  " + respJson.eta.dropoff);
                    Console.WriteLine(" Runner Name a is:  " + respJson.runner.name);
                    Console.WriteLine(" Runner  Mobile  is:  " + respJson.runner.phone_number);
                    pydukaanInsertCallBackStatusIntoSnapShotTable(respJson.task_id);
                    pydukaanIUpdateCallBackStatusForRunnerAccepted(respJson.task_id, respJson.state, respJson.eta.pickup, 
                        respJson.eta.dropoff, respJson.runner.name, respJson.runner.phone_number,blobFileName);

                }

                else if (blobString.Contains("pickup_complete") || blobString.Contains("started_for_delivery") ||
                    blobString.Contains("reached_for_delivery"))
                {
                    DeliveryCallback respJson = JsonConvert.DeserializeObject<DeliveryCallback>(blobString);
                    Console.WriteLine(" Task Id is:  " + respJson.task_id);
                    Console.WriteLine(" State is:  " + respJson.state);
                    //Console.WriteLine(" Drop Eta s:  " + respJson.Eta_delivery.dropoff);
                    Console.WriteLine(" Runner Name a is:  " + respJson.runner.name);
                    Console.WriteLine(" Runner  Mobile  is:  " + respJson.runner.phone_number);
                    pydukaanInsertCallBackStatusIntoSnapShotTable(respJson.task_id);
                    pydukaanIUpdateCallBackStatusForDeliveredPickupComplete(respJson.task_id, respJson.state, 
                        respJson.runner.name, respJson.runner.phone_number, blobFileName);
                }
                else if (blobString.Contains("cancelled"))
                {
                    dunzoCancelOrder respJson = JsonConvert.DeserializeObject<dunzoCancelOrder>(blobString);
                    pydukaanInsertCallBackStatusIntoSnapShotTable(respJson.task_id);
                    pydukaanUpdateStatusToCancel(respJson.task_id, respJson.state,blobFileName);
                }
                else
                {
                    Console.WriteLine(" Unreconized state : Please check");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(" Something is wrong " + ex.Message);
            }
        }


        public class LocationsOrder
        {
            public long event_timestamp { get; set; }
            public string state { get; set; }
            public string reference_id { get; set; }
            public string type { get; set; }
            public string cancelled_by { get; set; }
            public string cancellation_reason { get; set; }
        }

        public class dunzoCancelOrder
        {
            public string event_type { get; set; }
            public string event_id { get; set; }
            public string task_id { get; set; }
            public string state { get; set; }
            public long event_timestamp { get; set; }
            public string cancelled_by { get; set; }
            public string cancellation_reason { get; set; }
            public long request_timestamp { get; set; }
            public string reference_id { get; set; }
            public List<LocationsOrder> locations_order { get; set; }
            public string current_pickup_id { get; set; }
        }
        public static void pydukaanUpdateStatusToCancel(string task_id, string state, string blobFileName)
        {

            try
            {
                // Connect to Database and Get Max Merchant Id
                String connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                string queryJson = "update dunzoStatusTbl set dunzoStatus ='" + state + "', responseFileName = '" + blobFileName + "' where task_id='" + task_id + "'";
                //Console.WriteLine(queryJson);
                //opening the connection to Database
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    //executing the sql query 
                    using (SqlCommand cmd = new SqlCommand(queryJson, con))
                    {
                        con.Open();
                        var reader = cmd.ExecuteReader();
                    }
                    con.Close();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Exception pydukaanUpdateStatusToCancel  " + ex.Message);
            }
        }

        public static void pydukaanIUpdateCallBackStatusForQueued(string task_id, string state, 
                                             int pickupeta, int dropeta, string blobFileName)
        {

            try
            {
                // Connect to Database and Get Max Merchant Id
                String connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                string queryJson = "update dunzoStatusTbl set dunzoStatus ='" +  state + "', " +
                    "PickUpEta=" + pickupeta + " , DropEta=" + dropeta + ", responseFileName='" + blobFileName + "'" +
                    ",PickUpTime = dateadd(ss,19800+" + pickupeta * 60 + ",getdate())" +
                    ",dropTime = dateadd(ss,19800+" + (pickupeta + dropeta) * 60 + ",getdate()) " +  " where task_id='" + task_id + "'";
                //Console.WriteLine(queryJson);
                //opening the connection to Database
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    //executing the sql query 
                    using (SqlCommand cmd = new SqlCommand(queryJson, con))
                    {
                        con.Open();
                        var reader = cmd.ExecuteReader();
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Exception pydukaanIUpdateCallBackStatusForQueued  " + ex.Message);
            }

        }

        public static void pydukaanIUpdateCallBackStatusForRunnerAccepted(string task_id, string state,
                                             int pickup, int dropoff, 
                                             string runnerName, string runnerPhoneNum, string blobFileName)
        {

            try
            {
                // Connect to Database and Get Max Merchant Id
                String connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                string queryJson = "update dunzoStatusTbl set dunzoStatus ='" + state + "', " + 
                    "PickUpEta=" + pickup + " , DropEta=" + dropoff +  ", RunnerName='" + runnerName + " " + "" +
                    " ', responseFileName='" + blobFileName + "'" + " ,RunnerMobileNum='" + runnerPhoneNum + "'"
                    + " where task_id='" + task_id + "'";
                //Console.WriteLine(queryJson);
                //opening the connection to Database
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    //executing the sql query 
                    using (SqlCommand cmd = new SqlCommand(queryJson, con))
                    {
                        con.Open();
                        var reader = cmd.ExecuteReader();
                    }
                    con.Close();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Exception pydukaanIUpdateCallBackStatusForRunnerAccepted  " + ex.Message);
            }

        }


        public static void pydukaanIUpdateCallBackStatusForDeliveredPickupComplete(string task_id, string state,
                        string runnerName, string runnerPhoneNum, string blobFileName)
        {

            try
            {
                // Connect to Database and Get Max Merchant Id
                String connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                string queryJson = "update dunzoStatusTbl set dunzoStatus ='" + state + "', " +
                    " RunnerName='" + runnerName + "'"  +
                    " ,responseFileName='" + blobFileName + "'" + " ,RunnerMobileNum='" + runnerPhoneNum + "'"
                    + " where task_id='" + task_id + "'";
                //Console.WriteLine(queryJson);
                //opening the connection to Database
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    //executing the sql query 
                    using (SqlCommand cmd = new SqlCommand(queryJson, con))
                    {
                        con.Open();
                        var reader = cmd.ExecuteReader();
                    }
                    con.Close();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Exception pydukaanIUpdateCallBackStatusForQueued  " + ex.Message);
            }

        }




        public static void pydukaanInsertCallBackStatusIntoSnapShotTable( string task_id)
        {

            try
            {
                // Connect to Database and Get Max Merchant Id
                String connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                string queryJson = "INSERT INTO [dbo].[dunzoStatusTbl_SnapShot]([IDdunzo],[customerinvoiceid]," +
                    "[invoiceId],[task_id]," + "[dunzoStatus],[estimatedPrice]," +
                    "[PickUpEta],[DropEta],[PickUpTime],[DropTime],[respStr],[responseFileName],[CreatedDate]," +
                    "[RunnerName],[RunnerMobileNum],[snapShotTime])" +
                    " select[IDdunzo],[customerinvoiceid],[invoiceId],[task_id]," +
                    "[dunzoStatus],[estimatedPrice],[PickUpEta],[DropEta],[PickUpTime],[DropTime]," +
                    "[respStr],[responseFileName],[CreatedDate],[RunnerName]," +
                    "[RunnerMobileNum],getdate() from dunzoStatusTbl  where task_id ='" + task_id + "'";

                //Console.WriteLine(queryJson);
                //opening the connection to Database
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    //executing the sql query 
                    using (SqlCommand cmd = new SqlCommand(queryJson, con))
                    {
                        con.Open();
                        var reader = cmd.ExecuteReader();
                    }
                    con.Close();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Exception pydukaanInsertCallBackStatusIntoSnapShotTable  " + ex.Message);
            }

        }
        public static int pydukaanGetOrderIdFromTaskId(string taskid)
        {
            int customerInvoiceId = 0;
            try
            {
                // Connect to Database and Get Max Merchant Id
                String connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                string queryJson = "select customerinvoiceid from dunzoStatusTbl  " +
                    " where task_id= " + taskid;
                //opening the connection to Database
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    //executing the sql query 
                    using (SqlCommand cmd = new SqlCommand(queryJson, con))
                    {
                        con.Open();
                        var reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            customerInvoiceId = int.Parse(reader[0].ToString());
                        }
                    }
                    con.Close();
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(" Exception pydukaanGetOrderIdFromTaskId  " + ex.Message);
            }
            return customerInvoiceId;

        }
        public static CloudBlobContainer pydukaanBlobCreateContainer(string containerName)
        {
            CloudBlobContainer container = null;

            try
            {
                String storageConnection = ConfigurationManager.AppSettings["StorageConnectionPy"];
                CloudStorageAccount storageAccount =
                CloudStorageAccount.Parse(storageConnection);
                // Create the blob client.
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                // Retrieve a reference to a container.
                container = blobClient.GetContainerReference(containerName);
                // Create the container if it doesn't already exist.
                container.CreateIfNotExists();
                container.SetPermissions(new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                });
            }
            catch (System.Exception e)
            {
                Console.WriteLine(" Blob Not able to connect to Azure");
            }
            return container;
        }
        public static MemoryStream pydukaanDownloadJsonFile(string fileName, CloudBlobContainer blobContainer)
        {
            try
            {
                CloudBlob appBlob = blobContainer.GetBlobReference(fileName);
                var ms = new MemoryStream();
                appBlob.DownloadToStream(ms);
                //You have to rewind the MemoryStream before copying
                ms.Seek(0, SeekOrigin.Begin);
                return ms;
            }
            catch (Exception e)
            {
                Console.WriteLine(" Exception found in download customer list file funciton : " + e.Message);
                return null;
            }
        }
        public static string pydukaanGetContainerName(int merchantId)
        {
            string ContainerName = "pydukaan-" + merchantId;
            try
            {
                return ContainerName;
            }
            catch (Exception e)
            {
                return null;
            }
        }
        public class Eta
        {
            public int pickup { get; set; }
            public int dropoff { get; set; }
        }
        public class queuedRunner_cancelled
        {
            public string event_type { get; set; }
            public string event_id { get; set; }
            public string task_id { get; set; }
            public string reference_id { get; set; }
            public string state { get; set; }
            public long event_timestamp { get; set; }
            public Eta eta { get; set; }
            public long request_timestamp { get; set; }
        }

        public class Location
        {
            public double lat { get; set; }
            public double lng { get; set; }
        }

        public class Runner
        {
            public string name { get; set; }
            public string phone_number { get; set; }
            public Location location { get; set; }
        }

        public class runner_acceptedReached_for_pickup
        {
            public string event_type { get; set; }
            public string event_id { get; set; }
            public string task_id { get; set; }
            public string reference_id { get; set; }
            public string state { get; set; }
            public long event_timestamp { get; set; }
            public Eta eta { get; set; }
            public Runner runner { get; set; }
            public long request_timestamp { get; set; }
        }



        public class Eta_delivery
        {
            public int dropoff { get; set; }
        }
        
        public class DeliveryCallback
        {
            public string event_type { get; set; }
            public string event_id { get; set; }
            public string task_id { get; set; }
            public string reference_id { get; set; }
            public string state { get; set; }
            public long event_timestamp { get; set; }
            public Eta_delivery Eta_delivery { get; set; }
            public Runner runner { get; set; }
            public long request_timestamp { get; set; }
            public string tracking_url { get; set; }
        }

    }
}
