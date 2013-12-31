using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataSearch
{
    public interface IBigDataIndexer
    {
        void Init();
        void Add(string val, FileBuffer fileBuffer);
        void CreateIndexFile();

        FileBuffer[] GetIndices(string val);
    }
}
