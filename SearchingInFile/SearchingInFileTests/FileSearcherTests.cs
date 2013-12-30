using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using SearchingInFile;

namespace SearchingInFileTests
{
    [TestClass]
    public class FileSearcherTests
    {
        [TestMethod]
        [Description("Test Init method when file is not exits. Expected result: false and error message")]
        public void TestInit_NoSuchFile_ReturnFalse()
        {
            //Arrange:
            FileSearcher fileSearcher = new FileSearcher("NoSuchFile");

            //Act:
            bool result = fileSearcher.Init();

            //Assert:
            Assert.IsFalse(result);
            Assert.AreEqual(fileSearcher.ErrorMessage, "File cannot be opened");
        }

        [TestMethod]
        [Description("Test Init method when file exits. Expected results: true and empty error message")]
        public void TestInit_FileExits_ReturnTrue()
        {
            //Arrange:
            string filePath = @"..\..\EmptyFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);

            //Act:
            bool result = fileSearcher.Init();

            //Assert:
            Assert.IsTrue(result);
            Assert.IsTrue(string.IsNullOrEmpty(fileSearcher.ErrorMessage));
        }


        [TestMethod]
        [Description("Test Search method when there is no such email. Expected result: empty list")]
        public void TestSearch_NoSuchEmail_ReturnEmptyList()
        {
            //Arrange:
            string filePath = @"..\..\EmptyFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);
            bool initResult = fileSearcher.Init();

            //Act:
            List<int> result = fileSearcher.Search("no@such.email");
            
            //Assert:
            Assert.AreEqual(result.Count, 0);
        }

        [TestMethod]
        [Description("Test Search method when there is no such name. Expected result: empty list")]
        public void TestSearch_NoSuchName_ReturnEmptyList()
        {
            //Arrange:
            string filePath = @"..\..\EmptyFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);
            bool initResult = fileSearcher.Init();

            //Act:
            List<int> result = fileSearcher.Search("No Such Name");

            //Assert:
            Assert.AreEqual(result.Count, 0);
        }

        [TestMethod]
        [Description("Test Search in file when given a null string. Expected result: empty list")]
        public void TestSearch_NullString_ReturnEmptyList()
        {
            //Arrange:
            FileSearcher fileSearcher = new FileSearcher("NoSuchFile");

            //Act:
            List<int> result = fileSearcher.Search(null);

            //Assert:
            Assert.AreEqual(result.Count, 0);
        }

        [TestMethod]
        [Description("Test Search in file when given an empty string. Expected result: empty list")]
        public void TestSearch_EmptyString_ReturnEmptyList()
        {
            //Arrange:
            FileSearcher fileSearcher = new FileSearcher("NoSuchFile");

            //Act:
            List<int> result = fileSearcher.Search("");

            //Assert:
            Assert.AreEqual(result.Count, 0);
        }

        [TestMethod]
        [Description("Test Search method when there is only one line for the email. Expected result: list with one id")]
        public void TestSearch_SingleEmailOccurance_ReturnListWithOneId()
        {
            //Arrange:
            string filePath = @"..\..\SearchInFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);
            bool initResult = fileSearcher.Init();

            //Act:
            List<int> result = fileSearcher.Search("Denis@Dentler.com");

            //Assert:
            Assert.AreEqual(result.Count, 1);
            Assert.AreEqual(result[0], 25);
        }

        [TestMethod]
        [Description("Test Search method when there are multiple lines for the same email. Expected result: list with three items")]
        public void TestSearch_MultipleEmailOccurances_ReturnListWithThreeId()
        {
            //Arrange:
            string filePath = @"..\..\SearchInFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);
            bool initResult = fileSearcher.Init();

            //Act:
            List<int> result = fileSearcher.Search("Hazel@Zemke.com");

            //Assert:
            Assert.AreEqual(result.Count, 3);
        }


        [TestMethod]
        [Description("Test Search method when there is only one line for the name. Expected result: list with one id")]
        public void TestSearch_SingleNameOccurance_ReturnListWithOneId()
        {
            //Arrange:
            string filePath = @"..\..\SearchInFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);
            bool initResult = fileSearcher.Init();

            //Act:
            List<int> result = fileSearcher.Search("Denis Dentler");

            //Assert:
            Assert.AreEqual(result.Count, 1);
            Assert.AreEqual(result[0], 25);
        }

        [TestMethod]
        [Description("Test Search method when there are multiple lines for the same name. Expected result: list with three items")]
        public void TestSearch_MultipleNameOccurances_ReturnListWithThreeId()
        {
            //Arrange:
            string filePath = @"..\..\SearchInFile.csv";
            FileSearcher fileSearcher = new FileSearcher(filePath);
            bool initResult = fileSearcher.Init();

            //Act:
            List<int> result = fileSearcher.Search("Jane Cockrum");

            //Assert:
            Assert.AreEqual(result.Count, 4);
        }

    }
}


