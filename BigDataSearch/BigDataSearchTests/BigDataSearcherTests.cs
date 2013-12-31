using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using BigDataSearch;
using System.IO;

namespace BigDataSearchTests
{
    [TestClass]
    public class BigDataSearcherTests
    {
        static string fileName = "Users.csv";
        static BigDataSearcher bigDataSearcher;
    
        public BigDataSearcherTests()
        {

        }

        [ClassInitialize]
        public static void init(TestContext context)
        {
            bigDataSearcher = new BigDataSearcher(Environment.CurrentDirectory + "..\\..\\..\\" + fileName);
            bigDataSearcher.Prepare();
        }

        [ClassCleanup]
        public static void CleanUp()
        {
            bigDataSearcher.Dispose();
            foreach (FileInfo f in new DirectoryInfo(Environment.CurrentDirectory + "..\\..\\..\\").GetFiles("*.csv"))
            {
                if(! f.Name.Equals(fileName))
                    f.Delete();
            }
        }

        [TestMethod]
        [Description("Test SearchByZipCode with null string. Get empty query result")]
        public void SearchByZipCode_NullArgument_EmptyQueryResult()
        {
            //Arrange:
            string zipCode = null;

            //Act:
            QueryResult result = bigDataSearcher.SearchByZipCode(zipCode);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, string.Empty);
            Assert.AreEqual(result.NumberOfRecord, 0);
        }


        [TestMethod]
        [Description("Test SearchByZipCode with empty string. Get empty query result")]
        public void SearchByZipCode_EmptyArgument_EmptyQueryResult()
        {
            //Arrange:
            string zipCode = "";

            //Act:
            QueryResult result = bigDataSearcher.SearchByZipCode(zipCode);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, string.Empty);
            Assert.AreEqual(result.NumberOfRecord, 0);
        }

        [TestMethod]
        [Description("Test SearchByZipCode with a zip code that does not exist. Get empty query result")]
        public void SearchByZipCode_NoSuchZipCode_OneRecord()
        {
            //Arrange:
            string zipCode = "123456";

            //Act:
            QueryResult result = bigDataSearcher.SearchByZipCode(zipCode);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, string.Empty);
            Assert.AreEqual(result.NumberOfRecord, 0);
        }

        [TestMethod]
        [Description("Test SearchByZipCode with a zip code that should return query with only one record string. Get single record query result")]
        public void SearchByZipCode_ZipCode_OneRecord()
        {
            //Arrange:
            string zipCode = "08846";
            string expectedRecord = "\"Alisha\",\"Slusarski\",\"Wtlz Power 107 Fm\",\"3273 State St\",\"Middlesex\",\"Middlesex\",\"NJ\",\"08846\",\"732-658-3154\",\"732-635-3453\",\"alisha@slusarski.com\",\"http://www.wtlzpowerfm.com\"\n";

            //Act:
            QueryResult result = bigDataSearcher.SearchByZipCode(zipCode);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, expectedRecord);
            Assert.AreEqual(result.NumberOfRecord, 1);
        }

        [TestMethod]
        [Description("Test SearchByZipCode with a zip code that has two records. Get two records in query result")]
        public void SearchByZipCode_ZipCode_TwoRecords()
        {
            //Arrange:
            string zipCode = "85013";
            string firstRecord = "\"Mattie\",\"Poquette\",\"Century Communications\",\"73 State Road 434 E\",\"Phoenix\",\"Maricopa\",\"AZ\",85013,\"602-277-4385\",\"602-953-6360\",\"mattie@aol.com\",\"http://www.centurycommunications.com\"";
            string secondRecord = "\"Elke\",\"Sengbusch\",\"Riley Riper Hollin & Colagreco\",\"9 W Central Ave\",\"Phoenix\",\"Maricopa\",\"AZ\",85013,\"602-896-2993\",\"602-575-3457\",\"elke_sengbusch@yahoo.com\",\"http://www.rileyriperhollincolagreco.com\"";

            //Act:
            QueryResult result = bigDataSearcher.SearchByZipCode(zipCode);
            string[] tokens = result.Result.Split('\n');
            string firstActual = tokens[0];
            string secondActual = tokens[1];

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(firstRecord, firstActual);
            Assert.AreEqual(secondRecord, secondActual);
            Assert.AreEqual(result.NumberOfRecord, 2);
        }



        [TestMethod]
        [Description("Test SearchByEmail with empty string. Get empty query result")]
        public void SearchByEmail_EmptyArgument_EmptyQueryResult()
        {
            //Arrange:
            string email = "";

            //Act:
            QueryResult result = bigDataSearcher.SearchByEmail(email);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, string.Empty);
            Assert.AreEqual(result.NumberOfRecord, 0);
        }

        [TestMethod]
        [Description("Test SearchByEmail with null string. Get empty query result")]
        public void SearchByEmail_NullArgument_EmptyQueryResult()
        {
            //Arrange:
            string email = null;

            //Act:
            QueryResult result = bigDataSearcher.SearchByEmail(email);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, string.Empty);
            Assert.AreEqual(result.NumberOfRecord, 0);
        }


        [TestMethod]
        [Description("Test SearchByEmail with an email that does not exist. Get empty query result")]
        public void SearchByEmail_NoSuchEmail_OneRecord()
        {
            //Arrange:
            string email = "abcdefghijk@lmnopqrst.uvwxyz";

            //Act:
            QueryResult result = bigDataSearcher.SearchByEmail(email);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, string.Empty);
            Assert.AreEqual(result.NumberOfRecord, 0);
        }


        [TestMethod]
        [Description("Test SearchByEmail with a valid email that should return query with only one record string. Get single record query result")]
        public void SearchByEmail_ValidEmail_OneRecord()
        {
            //Arrange:
            string email = "alisha@slusarski.com";
            string expectedRecord = "\"Alisha\",\"Slusarski\",\"Wtlz Power 107 Fm\",\"3273 State St\",\"Middlesex\",\"Middlesex\",\"NJ\",\"08846\",\"732-658-3154\",\"732-635-3453\",\"alisha@slusarski.com\",\"http://www.wtlzpowerfm.com\"";

            //Act:
            QueryResult result = bigDataSearcher.SearchByEmail(email);

            //Assert:
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Result, expectedRecord);
            Assert.AreEqual(result.NumberOfRecord, 1);
        }
    }
}
