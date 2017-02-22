using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.Batch.Jeff.Common
{
    public class AppSettings
    {
        public string BatchAccountName { get; set; }
        public string BatchAccountKey { get; set; }
        public string BatchServiceUrl { get; set; }
        public string StorageAccountName { get; set; }
        public string StorageAccountKey { get; set; }
        public string StorageServiceUrl { get; set; }

        //public string NumberOfTasks { get; set; }
        //public string PoolNodeCount { get; set; }
        public string FileName { get; set; }
        public List<string> Files { get; set; }
        public string PoolId { get; set; }
        public string JobId { get; set; }


        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();

            AddSetting(stringBuilder, "BatchAccountName", this.BatchAccountName);
            AddSetting(stringBuilder, "BatchAccountKey", this.BatchAccountKey);
            AddSetting(stringBuilder, "BatchServiceUrl", this.BatchServiceUrl);

            AddSetting(stringBuilder, "StorageAccountName", this.StorageAccountName);
            AddSetting(stringBuilder, "StorageAccountKey", this.StorageAccountKey);
            AddSetting(stringBuilder, "StorageServiceUrl", this.StorageServiceUrl);

            return stringBuilder.ToString();
        }
        private static void AddSetting(StringBuilder stringBuilder, string settingName, object settingValue)
        {
            stringBuilder.AppendFormat("{0} = {1}", settingName, settingValue).AppendLine();
        }
    }
}
