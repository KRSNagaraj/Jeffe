using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.Jeff.Common;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jeff_ProcessFiles
{
    class Program
    {
        static AppSettings settings = new AppSettings();
        static void Main(string[] args)
        {
            RunJob.StartProcess(args);
            ProcessFiles jb = new ProcessFiles();
            jb.DownloadFromAzureStorage();

            LoadAppSettings(settings);
            Runjob(args);

            if (args != null && args.Length > 0 && args[0] == "--Task")
            {
                ProcessFiles.TaskMain(args);
            }
            else
            {
                Job.JobMain(args);
            }
        }

        private static void Runjob(string[] args)
        {
            try
            {
                 MainAsync(args).Wait();
            }
            catch (AggregateException ae)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();

                PrintAggregateException(ae.Flatten());
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("File Process completed, ENTER to exit...");
                Console.ReadLine();
            }
        }
        public static void PrintAggregateException(AggregateException aggregateException)
        {
            // Go through all exceptions and dump useful information
            foreach (Exception exception in aggregateException.InnerExceptions)
            {
                Console.WriteLine(exception.ToString());
                Console.WriteLine();
            }
        }

        private static void LoadAppSettings(AppSettings settings)
        {
            settings.BatchAccountName = ConfigurationManager.AppSettings["BatchAccountName"].ToString();
            settings.BatchAccountKey = ConfigurationManager.AppSettings["BatchAccountKey"].ToString();
            settings.BatchServiceUrl = ConfigurationManager.AppSettings["BatchServiceUrl"].ToString();
            settings.StorageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"].ToString();
            settings.StorageAccountName = ConfigurationManager.AppSettings["StorageAccountName"].ToString();
            settings.StorageServiceUrl = ConfigurationManager.AppSettings["StorageServiceUrl"].ToString();
        }
        private static async Task MainAsync(string[] args)
        {
            // You may adjust these values to experiment with different compute resource scenarios.
            const string nodeSize = "small";
            const int nodeCount = 2;
            const int maxTasksPerNode = 2;
            const int taskCount = 12;

            // Ensure there are enough tasks to help avoid hitting some timeout conditions below
            int minimumTaskCount = nodeCount * maxTasksPerNode * 2;
            if (taskCount < minimumTaskCount)
            {
                Console.WriteLine("You must specify at least two tasks per node core for this sample ({0} tasks in this configuration).", minimumTaskCount);
                Console.WriteLine();

                // Not enough tasks, exit the application
                return;
            }

            // In this sample, the tasks simply ping localhost on the compute nodes; adjust these
            // values to simulate variable task duration
            const int minPings = 30;
            const int maxPings = 60;

            const string poolId = "ParallelTasksSamplePool";
            const string jobId = "ParallelTasksSampleJob";

            // Amount of time to wait before timing out (potentially) long-running tasks
            TimeSpan longTaskDurationLimit = TimeSpan.FromMinutes(10);

            // Set up access to your Batch account with a BatchClient. Configure your AccountSettings in the
            // Microsoft.Azure.Batch.Samples.Common project within this solution.
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(settings.BatchServiceUrl,
                                                                           settings.BatchAccountName,
                                                                           settings.BatchAccountKey);

            using (BatchClient batchClient = await BatchClient.OpenAsync(cred))
            {
                // Create a CloudPool, or obtain an existing pool with the specified ID
                CloudPool pool = await ArticleHelpers.CreatePoolIfNotExistAsync(batchClient,
                                                                      poolId,
                                                                      nodeSize,
                                                                      nodeCount,
                                                                      maxTasksPerNode);

                // Create a CloudJob, or obtain an existing pool with the specified ID
                CloudJob job = await ArticleHelpers.CreateJobIfNotExistAsync(batchClient, poolId, jobId);

                // The job's tasks ping localhost a random number of times between minPings and maxPings.
                // Adjust the minPings/maxPings values above to experiment with different task durations.
                Random rand = new Random();
                List<CloudTask> tasks = new List<CloudTask>();
                for (int i = 1; i <= taskCount; i++)
                {
                    string taskId = "task" + i.ToString().PadLeft(3, '0');
                    string taskCommandLine = "ping -n " + rand.Next(minPings, maxPings + 1).ToString() + " localhost";
                    CloudTask task = new CloudTask(taskId, taskCommandLine);
                    tasks.Add(task);
                }

                // Pause execution until the pool is steady and its compute nodes are ready to accept jobs.
                // NOTE: Such a pause is not necessary within your own code. Tasks can be added to a job at any point and will be 
                // scheduled to execute on a compute node as soon any node has reached Idle state. Because the focus of this sample 
                // is the demonstration of running tasks in parallel on multiple compute nodes, we wait for all compute nodes to 
                // complete initialization and reach the Idle state in order to maximize the number of compute nodes available for 
                // parallelization.
                await ArticleHelpers.WaitForPoolToReachStateAsync(batchClient, pool.Id, AllocationState.Steady, longTaskDurationLimit);
                await ArticleHelpers.WaitForNodesToReachStateAsync(batchClient, pool.Id, ComputeNodeState.Idle, longTaskDurationLimit);

                // Add the tasks in one API call as opposed to a separate AddTask call for each. Bulk task submission
                // helps to ensure efficient underlying API calls to the Batch service.
                await batchClient.JobOperations.AddTaskAsync(job.Id, tasks);

                // Pause again to wait until *all* nodes are running tasks
                await ArticleHelpers.WaitForNodesToReachStateAsync(batchClient, pool.Id, ComputeNodeState.Running, TimeSpan.FromMinutes(2));

                Stopwatch stopwatch = Stopwatch.StartNew();

                // Print out task assignment information.
                Console.WriteLine();
                await GettingStartedCommon.PrintNodeTasksAsync(batchClient, pool.Id);
                Console.WriteLine();

                // Pause execution while we wait for all of the tasks to complete
                Console.WriteLine("Waiting for task completion...");
                Console.WriteLine();

                try
                {
                    await batchClient.Utilities.CreateTaskStateMonitor().WhenAll(
                        job.ListTasks(),
                        TaskState.Completed,
                        longTaskDurationLimit);
                }
                catch (TimeoutException e)
                {
                    Console.WriteLine(e.ToString());
                }

                stopwatch.Stop();

                // Obtain the tasks, specifying a detail level to limit the number of properties returned for each task.
                // If you have a large number of tasks, specifying a DetailLevel is extremely important in reducing the
                // amount of data transferred, lowering your query response times in increasing performance.
                ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id,commandLine,nodeInfo,state");
                IPagedEnumerable<CloudTask> allTasks = batchClient.JobOperations.ListTasks(job.Id, detail);

                // Get a collection of the completed tasks sorted by the compute nodes on which they executed
                List<CloudTask> completedTasks = allTasks
                                                .Where(t => t.State == TaskState.Completed)
                                                .OrderBy(t => t.ComputeNodeInformation.ComputeNodeId)
                                                .ToList();

                // Print the completed task information
                Console.WriteLine();
                Console.WriteLine("Completed tasks:");
                string lastNodeId = string.Empty;
                foreach (CloudTask task in completedTasks)
                {
                    if (!string.Equals(lastNodeId, task.ComputeNodeInformation.ComputeNodeId))
                    {
                        Console.WriteLine();
                        Console.WriteLine(task.ComputeNodeInformation.ComputeNodeId);
                    }

                    lastNodeId = task.ComputeNodeInformation.ComputeNodeId;

                    Console.WriteLine("\t{0}: {1}", task.Id, task.CommandLine);
                }

                // Get a collection of the uncompleted tasks which may exist if the TaskMonitor timeout was hit
                List<CloudTask> uncompletedTasks = allTasks
                                                   .Where(t => t.State != TaskState.Completed)
                                                   .OrderBy(t => t.Id)
                                                   .ToList();

                // Print a list of uncompleted tasks, if any
                Console.WriteLine();
                Console.WriteLine("Uncompleted tasks:");
                Console.WriteLine();
                if (uncompletedTasks.Any())
                {
                    foreach (CloudTask task in uncompletedTasks)
                    {
                        Console.WriteLine("\t{0}: {1}", task.Id, task.CommandLine);
                    }
                }
                else
                {
                    Console.WriteLine("\t<none>");
                }

                // Print some summary information
                Console.WriteLine();
                Console.WriteLine("             Nodes: " + nodeCount);
                Console.WriteLine("         Node size: " + nodeSize);
                Console.WriteLine("Max tasks per node: " + pool.MaxTasksPerComputeNode);
                Console.WriteLine("             Tasks: " + tasks.Count);
                Console.WriteLine("          Duration: " + stopwatch.Elapsed);
                Console.WriteLine();
                Console.WriteLine("Done!");
                Console.WriteLine();

                // Clean up the resources we've created in the Batch account
                Console.WriteLine("Delete job? [yes] no");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    await batchClient.JobOperations.DeleteJobAsync(job.Id);
                }

                Console.WriteLine("Delete pool? [yes] no");
                response = Console.ReadLine();
                if (response != "n" && response != "no")
                {
                    await batchClient.PoolOperations.DeletePoolAsync(pool.Id);
                }
            }
        }
    }
}
