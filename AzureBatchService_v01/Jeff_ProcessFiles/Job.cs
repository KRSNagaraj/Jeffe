using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.FileStaging;
using Microsoft.Azure.Batch.Jeff.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jeff_ProcessFiles
{
    public static class Job
    {
        // files that are required on the compute nodes that run the tasks
        private const string FileProcessExeName = "create_tests.exe";
        private const string StorageClientDllName = "Microsoft.WindowsAzure.Storage.dll";
        private const int PoolNodeCount = 2;
        private const int NumberOfTasks = 1;
        

        static AppSettings settings = new AppSettings();

        public static void JobMain(string[] args)
        {
            loadAppSettings(settings);
  
            CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(
                new StorageCredentials(
                    settings.StorageAccountName,
                    settings.StorageAccountKey),
                settings.StorageServiceUrl,
                useHttps: true);

            StagingStorageAccount stagingStorageAccount = new StagingStorageAccount(
                settings.StorageAccountName,
                settings.StorageAccountKey,
                cloudStorageAccount.BlobEndpoint.ToString());

            using (BatchClient client = BatchClient.Open(new BatchSharedKeyCredentials(settings.BatchServiceUrl, settings.BatchAccountName, settings.BatchAccountKey)))
            {
                string stagingContainer = null;

                //OSFamily 4 == OS 2012 R2. You can learn more about os families and versions at:
                //http://msdn.microsoft.com/en-us/library/azure/ee924680.aspx
                CloudPool pool = client.PoolOperations.CreatePool(
                    settings.PoolId,
                    targetDedicated: PoolNodeCount,
                    virtualMachineSize: "small",
                    cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));
                Console.WriteLine("Adding pool {0}", settings.PoolId);

                try
                {
                    pool.Commit();
                }
                catch (AggregateException ae)
                {
                    // Go through all exceptions and dump useful information
                    ae.Handle(x =>
                    {
                        Console.Error.WriteLine("Creating pool ID {0} failed", settings.PoolId);
                        if (x is BatchException)
                        {
                            BatchException be = x as BatchException;

                            Console.WriteLine(be.ToString());
                            Console.WriteLine();
                        }
                        else
                        {
                            Console.WriteLine(x);
                        }

                        // can't continue without a pool
                        return false;
                    });
                }


                try
                {
                    Console.WriteLine("Creating job: " + settings.JobId);
                    // get an empty unbound Job
                    CloudJob unboundJob = client.JobOperations.CreateJob();
                    unboundJob.Id = settings.JobId;
                    unboundJob.PoolInformation = new PoolInformation() { PoolId = settings.PoolId };

                    // Commit Job to create it in the service
                    unboundJob.Commit();

                    // create file staging objects that represent the executable and its dependent assembly to run as the task.
                    // These files are copied to every node before the corresponding task is scheduled to run on that node.
                    FileToStage ProcessExe = new FileToStage(FileProcessExeName, stagingStorageAccount);
                    FileToStage storageDll = new FileToStage(StorageClientDllName, stagingStorageAccount);

                    // In this sample, the input data is copied separately to Storage and its URI is passed to the task as an argument.
                    // This approach is appropriate when the amount of input data is large such that copying it to every node via FileStaging
                    // is not desired and the number of tasks is small since a large number of readers of the blob might get throttled
                    // by Storage which will lengthen the overall processing time.
                    //
                    // You'll need to observe the behavior and use published techniques for finding the right balance of performance versus
                    // complexity.
                    string bookFileUri = UploadFileToCloudBlob(settings, settings.FileName);
                    Console.WriteLine("{0} uploaded to cloud", settings.FileName);

                    // initialize a collection to hold the tasks that will be submitted in their entirety
                    List<CloudTask> tasksToRun = new List<CloudTask>(NumberOfTasks);

                    for (int i = 1; i <= NumberOfTasks; i++)
                    {
                        CloudTask task = new CloudTask("task_no_" + i, String.Format("{0} --Task {1} {2} {3} {4}",
                            FileProcessExeName,
                            bookFileUri,
                            10,
                            settings.StorageAccountName,
                            settings.StorageAccountKey));

                        //This is the list of files to stage to a container -- for each job, one container is created and 
                        //files all resolve to Azure Blobs by their name (so two tasks with the same named file will create just 1 blob in
                        //the container).
                        task.FilesToStage = new List<IFileStagingProvider>
                                            {
                                                ProcessExe,
                                                storageDll
                                            };

                        tasksToRun.Add(task);
                    }

                    // Commit all the tasks to the Batch Service. Ask AddTask to return information about the files that were staged.
                    // The container information is used later on to remove these files from Storage.
                    ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>> fsArtifactBag = new ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>>();
                    client.JobOperations.AddTask(settings.JobId, tasksToRun, fileStagingArtifacts: fsArtifactBag);

                    // loop through the bag of artifacts, looking for the one that matches our staged files. Once there,
                    // capture the name of the container holding the files so they can be deleted later on if that option
                    // was configured in the settings.
                    foreach (var fsBagItem in fsArtifactBag)
                    {
                        IFileStagingArtifact fsValue;
                        if (fsBagItem.TryGetValue(typeof(FileToStage), out fsValue))
                        {
                            SequentialFileStagingArtifact stagingArtifact = fsValue as SequentialFileStagingArtifact;
                            if (stagingArtifact != null)
                            {
                                stagingContainer = stagingArtifact.BlobContainerCreated;
                                Console.WriteLine(
                                    "Uploaded files to container: {0} -- you will be charged for their storage unless you delete them.",
                                    stagingArtifact.BlobContainerCreated);
                            }
                        }
                    }

                    //Get the job to monitor status.
                    CloudJob job = client.JobOperations.GetJob(settings.JobId);

                    Console.Write("Waiting for tasks to complete ...   ");
                    // Wait 20 minutes for all tasks to reach the completed state. The long timeout is necessary for the first
                    // time a pool is created in order to allow nodes to be added to the pool and initialized to run tasks.
                    IPagedEnumerable<CloudTask> ourTasks = job.ListTasks(new ODATADetailLevel(selectClause: "id"));
                    client.Utilities.CreateTaskStateMonitor().WaitAll(ourTasks, TaskState.Completed, TimeSpan.FromMinutes(20));
                    Console.WriteLine("tasks are done.");

                    foreach (CloudTask t in ourTasks)
                    {
                        Console.WriteLine("Task " + t.Id);
                        //Console.WriteLine("stdout:" + Environment.NewLine + t.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
                        //Console.WriteLine();
                        //Console.WriteLine("stderr:" + Environment.NewLine + t.GetNodeFile(Constants.StandardErrorFileName).ReadAsString());
                    }
                }
                finally
                {
                    //Delete the pool that we created
                  
                        Console.WriteLine("Deleting pool: {0}", settings.PoolId);
                        client.PoolOperations.DeletePool(settings.PoolId);

                    //Delete the job that we created
                 
                        Console.WriteLine("Deleting job: {0}", settings.JobId);
                        client.JobOperations.DeleteJob(settings.JobId);

                    //Delete the containers we created

                        DeleteContainers(settings, stagingContainer);
                }
            }
        }

        private static void loadAppSettings(AppSettings settings)
        {
             settings.BatchAccountName = ConfigurationManager.AppSettings["BatchAccountName"].ToString();
             settings.BatchAccountKey = ConfigurationManager.AppSettings["BatchAccountKey"].ToString();
             settings.BatchServiceUrl  = ConfigurationManager.AppSettings["BatchServiceUrl"].ToString();
             settings.StorageAccountKey  = ConfigurationManager.AppSettings["StorageAccountKey"].ToString();
             settings.StorageAccountName = ConfigurationManager.AppSettings["StorageAccountName"].ToString();
             settings.StorageServiceUrl = ConfigurationManager.AppSettings["StorageServiceUrl"].ToString();
        }

        /// <summary>
        /// create a client for accessing blob storage
        /// </summary>
        private static CloudBlobClient GetCloudBlobClient(string accountName, string accountKey, string accountUrl)
        {
            StorageCredentials cred = new StorageCredentials(accountName, accountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(cred, accountUrl, useHttps: true);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();

            return client;
        }

        private static string FilesContainerName = "Files";

        /// <summary>
        /// Delete the containers in Azure Storage which are created by this sample.
        /// </summary>
        private static void DeleteContainers(AppSettings accountSettings, string fileStagingContainer)
        {
            CloudBlobClient client = GetCloudBlobClient(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                accountSettings.StorageServiceUrl);

            CloudBlobContainer container = client.GetContainerReference(FilesContainerName);
            Console.WriteLine("Deleting container: " + FilesContainerName);
            container.DeleteIfExists();

            //Delete the file staging container
            if (!string.IsNullOrEmpty(fileStagingContainer))
            {
                container = client.GetContainerReference(fileStagingContainer);
                Console.WriteLine("Deleting container: {0}", fileStagingContainer);
                container.DeleteIfExists();
            }
        }

        /// <summary>
        /// Upload a text file to a cloud blob.
        /// </summary>
        /// <param name="accountSettings">The account settings.</param>
        /// <param name="fileName">The name of the file to upload</param>
        /// <returns>The URI of the blob.</returns>
        private static string UploadFileToCloudBlob(AppSettings accountSettings, string fileName)
        {
            CloudBlobClient client = GetCloudBlobClient(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                accountSettings.StorageServiceUrl);

            CloudBlobContainer container = client.GetContainerReference(FilesContainerName);
            container.CreateIfNotExists();

            //Upload the blob.
            CloudBlockBlob blob = container.GetBlockBlobReference(fileName);
            blob.UploadFromFile(fileName, FileMode.Open);
            return blob.Uri.ToString();
        }
    }
}
