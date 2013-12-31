using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataSearch
{
    public class FileBuffer
    {
        public FileBuffer(ulong offset, ulong length)
        {
            Offset = offset;
            Length = length;
        }

        public ulong Offset { private set; get; }
        public ulong Length { private set; get; }
    }
}
