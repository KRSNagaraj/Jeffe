using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.Batch.Jeff.Common
{
    public enum CreatePoolResult
    {
        PoolExisted,
        CreatedNew,
        ResizedExisting,
    }
}
