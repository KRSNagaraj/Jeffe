using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jeff_ProcessFiles
{
    public class ProcessFiles
    {
        /// <summary>
        /// This class has the code for each task. The task reads the
        /// blob assigned to it and each file through call exe and writes
        /// them to standard out
        /// </summary>
        public static void TaskMain(string[] args)
        {

            string blobName = args[1];
            int numTopN = int.Parse(args[2]);
            string storageAccountName = args[3];
            string storageAccountKey = args[4];

            // open the cloud blob that contains the book
            var storageCred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(blobName), storageCred);
            //blob.StartCopyFromBlob()

            using (Stream memoryStream = new MemoryStream())
            {
                blob.DownloadToStream(memoryStream);
                memoryStream.Position = 0; //Reset the stream

            }
        }

        public int DownloadFromAzureStorage()
        {
            try
            {
                //  create Azure Storage
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                    "DefaultEndpointsProtocol=https;AccountName="+ ConfigurationManager.AppSettings["BatchAccountName"].ToString() 
                    + ";AccountKey="+ ConfigurationManager.AppSettings["BatchAccountKey"].ToString() );
                
                //  create a blob client.
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                //  create a container 
                CloudBlobContainer container = blobClient.GetContainerReference(
                    ConfigurationManager.AppSettings["ContainerName"].ToString());

                //  create a block blob
                CloudBlockBlob blockBlob = container.GetBlockBlobReference("create_tests.exe");
                //https://storageforbatchservice.blob.core.windows.net/myfirstbatchcontainer/create_tests.exe
                //  create a local file
                if (!Directory.Exists(@"path\"))
                {
                    Directory.CreateDirectory(@"path\");
                }
                // Save blob contents to a file.
                using (var fileStream = System.IO.File.OpenWrite(@"path\myfile.exe"))
                {
                    blockBlob.DownloadToStream(fileStream);
                }
                //  download from Azure Storage

                return 1;
            }
            catch
            {
                //  return error
                return 0;
            }
        }



    }
}
