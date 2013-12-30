using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataSearch
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();

            System.Console.WriteLine("Please enter file path:");
            var filePath = System.Console.ReadLine();
            if (!File.Exists(filePath))
            {
                System.Console.WriteLine("File not found.");
                return;
            }

            System.Console.WriteLine("Reading file: {0}", filePath);
            sw.Start();
            BigDataSearcher bigDataSearchHelper = new BigDataSearcher(filePath);
            bigDataSearchHelper.Prepare();
            sw.Stop(); 
            System.Console.WriteLine("Total prepare time: {0}\n", sw.Elapsed);

            printMenu();
            System.Console.Write(">");
            while (true)
            {
                string input = System.Console.ReadLine();
                switch (input)
                {
                    case("q"):
                        return;
                    case("e"):
                        searchByEmail(bigDataSearchHelper);
                        break;
                    case ("z"):
                        searchByZipcode(bigDataSearchHelper);
                        break;
                }
                printMenu();
                System.Console.Write(">");
            }
        }

        private static void searchByZipcode(BigDataSearcher bigDataSearchHelper)
        {
            System.Console.WriteLine("Please enter zip code");
            string zipCode = System.Console.ReadLine();
            BigDataSearcher.QueryResult result = bigDataSearchHelper.SearchByZipCode(zipCode);
            System.Console.WriteLine("Result=\n{0}", result.Result);
            System.Console.WriteLine("Total query time: {0}", result.ExecTime);
            System.Console.WriteLine("Total number of records: {0}", result.NumberOfRecord);
            System.Console.WriteLine();
        }

        private static void searchByEmail(BigDataSearcher bigDataSearchHelper)
        {
            System.Console.WriteLine("Please enter email");
            string email = System.Console.ReadLine();
            BigDataSearcher.QueryResult result = bigDataSearchHelper.SearchByEmail(email);
            System.Console.WriteLine("Result=\n{0}", result.Result);
            System.Console.WriteLine("Total query time: {0}", result.ExecTime);
            System.Console.WriteLine("Total number of records: {0}", result.NumberOfRecord);
            System.Console.WriteLine();
        }

        private static void printMenu()
        {
            System.Console.WriteLine("Please choose:");
            System.Console.WriteLine("e - search by email");
            System.Console.WriteLine("z - search by zip code");
            System.Console.WriteLine("q - quit");
        }
    }
}
