using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataSearch
{
    public interface IBigDataSearcher
    {

        /// <summary>
        /// Prepare the data base
        /// </summary>
        void Prepare();

        /// <summary>
        /// Query by zip code
        /// </summary>
        /// <param name="zipCode"></param>
        /// <returns></returns>
        QueryResult SearchByZipCode(string zipCode);

        /// <summary>
        /// Query by email
        /// </summary>
        /// <param name="email"></param>
        /// <returns></returns>
        QueryResult SearchByEmail(string email);
    }
}
