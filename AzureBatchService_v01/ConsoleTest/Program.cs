using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleTest
{
    class Program
    {
        // Storage account credentials
        private static string StorageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
        private static string StorageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"];

        static void Main(string[] args)
        {
            CopyInputFiles();
        }

        private static void CopyFiles()
        {
            const string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=juanktest;AccountKey=loHQwke4lSEu1p2W3gg==";
            const string container1 = "juankcontainer";
            const string sourceBlobName = "test.txt";
            const string destBlobName = "newTest.txt";
            

            //Setup Account, blobclient and blobs
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient blobClient = account.CreateCloudBlobClient();

            CloudBlobContainer blobContainer = blobClient.GetContainerReference(container1);
            blobContainer.CreateIfNotExists();

            CloudBlockBlob sourceBlob = blobContainer.GetBlockBlobReference(sourceBlobName);

            CloudBlockBlob destinationBlob = blobContainer.GetBlockBlobReference(destBlobName);

            ////Setup data transfer
            //TransferContext context = new TransferContext();
            //Progress<TransferProgress> progress = new Progress<TransferProgress>(
            //    (transferProgress) => {
            //        Console.WriteLine("Bytes uploaded: {0}", transferProgress.BytesTransferred);
            //    });

            //context.ProgressHandler = progress;

            //// Start the transfer
            //try
            //{
            //    TransferManager.CopyAsync(sourceBlob, destinationBlob,
            //        false /* isServiceCopy */,
            //        null /* options */, context);
            //}
            //catch (Exception e)
            //{
            //    Console.WriteLine("The transfer is cancelled: {0}", e.Message);
            //}

            Console.WriteLine("CloudBlob {0} is copied to {1} ====successfully====",
                            sourceBlob.Uri.ToString(),
                            destinationBlob.Uri.ToString());

            Console.ReadLine();
        }

        private static void CopyInputFiles()
        {
            string filePath = "/input/";
            string containerName = ConfigurationManager.AppSettings["InputContainerName"];
            string sourceBlobName = "InputFile_01.dat";
            string destBlobName = "InputFile_";
            

            // Construct the Storage account connection string
            string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                                            StorageAccountName, StorageAccountKey);

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client, for use in obtaining references to blob storage containers
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();


            CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);
            blobContainer.CreateIfNotExists();

            CloudBlockBlob sourceBlob = blobContainer.GetBlockBlobReference(sourceBlobName);

            for (int cnt =1; cnt <= 500; cnt++)
            {
                string blob = destBlobName + (cnt > 9 ? "" : "0") + cnt.ToString() + ".dat";

                CloudBlockBlob destinationBlob = blobContainer.GetBlockBlobReference(blob);

                destinationBlob.StartCopy(sourceBlob);
            }


            //string blobName = Path.GetFileName(filePath);

            //CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            ////CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            ////blobData.UploadFromFileAsync(filePath, FileMode.Open).Wait();


            //foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
            //{
            //    // Retrieve reference to the current blob
            //    CloudBlob blob = (CloudBlob)item;

            //    //// Save blob contents to a file in the specified folder
            //    //string localOutputFile = Path.Combine(directoryPath, blob.Name);
            //    //await blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            //}


        }
    }
}
