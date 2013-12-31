using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataSearch
{
    public class QueryResult
    {
        public string Result { get; set; }
        public long NumberOfRecord { get; set; }
        public TimeSpan ExecTime { get; set; }
    }
}
