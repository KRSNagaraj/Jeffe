using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RunFileProcessApp
{
    public class Program
    {
        private static string StorageAccountName = "storageforbatchservice";
        private static string StorageAccountKey = "9pJHNFp9P0rJZOOLorlLX01iMfmS3OmoWssL+dn6zLgw70DKjNGKjSN2YRYxirEwHn6pemFV5X9KcbyYzxFXag==";
        private static string ContainerName = "output";

        private static void LoadConfig()
        {
            StorageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
            StorageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"];

            ContainerName = ConfigurationManager.AppSettings["ContainerName"];

        }
     
        public static void Main(string[] args)
        {
            //test();

            // Construct the Storage account connection string
            string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                                            StorageAccountName, StorageAccountKey);

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client, for use in obtaining references to blob storage containers
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();


            string _sas = "https://storageforbatchservice.blob.core.windows.net/output?sv=2015-04-05&sr=c&sig=RzQFdc8gMoGvcbn36wUC7tcTf5Htm0Dkj0a6uu8NR54%3D&se=2017-02-22T20%3A49%3A25Z&sp=w";
            try
            {

                string ProcessExeFile = args[0];
                string inputFile = args[1];


                // The third argument should be the shared access signature for the container in Azure Storage
                // to which this task application will upload its output. This shared access signature should
                // provide WRITE access to the container.
                string outputContainerSas = args[2];

                System.Diagnostics.Process.Start("cmd.exe ", " /c " + ProcessExeFile + " " + inputFile).WaitForExit();

                // Send the output to text file
                string outputFile = inputFile + ".correct_info";

                string outputFile01 = String.Format("{0}_OUTPUT{1}", Path.GetFileNameWithoutExtension(inputFile), Path.GetExtension(inputFile));
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(outputFile01))
                {
                    file.WriteLine("cmd.exe /c ", ProcessExeFile + " " + inputFile);
                    file.WriteLine("ProcessExeFile->" + File.Exists(ProcessExeFile).ToString());
                    file.WriteLine("inputFile->" + File.Exists(inputFile).ToString());
                    file.WriteLine("outputFile->" + File.Exists(outputFile).ToString());


                    // Write out some task information using some of the node's environment variables
                    file.WriteLine("------------------------------");
                    file.WriteLine("Node: " + Environment.GetEnvironmentVariable("AZ_BATCH_NODE_ID"));
                    file.WriteLine("Task: " + Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID"));
                    file.WriteLine("Job:  " + Environment.GetEnvironmentVariable("AZ_BATCH_JOB_ID"));
                    file.WriteLine("Pool: " + Environment.GetEnvironmentVariable("AZ_BATCH_POOL_ID"));
                    //UploadFileToContainer(outputFile01, outputContainerSas);
                    file.Flush();
                    file.Close();

                    UploadFileToContainer(blobClient, outputFile, ContainerName);
                }

            }
            catch (Exception ex)
            {
                string outputFile01 = String.Format("Task_ERROR.log");

                using (System.IO.StreamWriter file = new System.IO.StreamWriter(outputFile01))
                {
                    //EventLog.WriteEntry("RunFileProcess", ex.Message + " -> " + ex.StackTrace, EventLogEntryType.Error);
                    file.WriteLine(ex.Message);
                    file.WriteLine(ex.StackTrace);
                    file.WriteLine(ex.InnerException);


                    // Write out some task information using some of the node's environment variables
                    file.WriteLine("------------------------------");
                    file.WriteLine("Node: " + Environment.GetEnvironmentVariable("AZ_BATCH_NODE_ID"));
                    file.WriteLine("Task: " + Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID"));
                    file.WriteLine("Job:  " + Environment.GetEnvironmentVariable("AZ_BATCH_JOB_ID"));
                    file.WriteLine("Pool: " + Environment.GetEnvironmentVariable("AZ_BATCH_POOL_ID"));
                    file.Flush();
                    file.Close();
                    UploadFileToContainer(blobClient, outputFile01, ContainerName);

                }

            }
            // Upload the output file to blob container in Azure Storage

        }

        /// <summary>
        /// Uploads the specified file to the container represented by the specified
        /// container shared access signature (SAS).
        /// </summary>
        /// <param name="filePath">The path of the file to upload to the Storage container.</param>
        /// <param name="containerSas">The shared access signature granting write access to the specified container.</param>
        private static void UploadFileToContainer(CloudBlobClient blobClient, string filePath, string containerName)
        {
            string blobName = Path.GetFileName(filePath);

            try
            {
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
                blobData.UploadFromFileAsync(filePath, FileMode.Open).Wait();
            }
            catch (StorageException e)
            {

                Console.WriteLine("Write operation failed for container URL " + containerName);
                Console.WriteLine("Additional error information: " + e.Message);
                Console.WriteLine();

                Environment.ExitCode = -1;
            }
        }
    }
}
