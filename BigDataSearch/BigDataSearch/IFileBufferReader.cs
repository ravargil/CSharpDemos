using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataSearch
{
    public interface IFileBufferReader
    {
        string Read(FileBuffer[] fileBuffers);
        string Read(FileBuffer fileBuffer);
    }
}
