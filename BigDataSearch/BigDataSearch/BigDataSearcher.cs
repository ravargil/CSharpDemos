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
    public class BigDataSearcher : IDisposable
    {
        public class QueryResult
        {
            public string Result { get; set; }
            public long NumberOfRecord { get; set; }
            public TimeSpan ExecTime { get; set; }
        }

        private string FirstPhaseZipCodeIndexFileName;
        private string FirstPhaseEmailIndexFileName;
        private string SortedFirstPhaseZipCodeIndexFileName;
        private string ZipCodeIndexFileName;
        private string SortedFileName;
        private string originalFileName;
        private string directory;
        private string originalFileNameWithoutExt;
        private string mergedEmailIndicesFileName;
        private string currentEmailIndicesFileInMemory;

        private MemoryMappedFile memoryMappedForSortedFile;
        private MemoryMappedFile memoryMappedForOriginalFile;
        private SortedDictionary<string, Tuple<long, long>> zipCodeIndices = new SortedDictionary<string, Tuple<long, long>>();
        private SortedDictionary<string, Tuple<long, long>> emailIndices = new SortedDictionary<string, Tuple<long, long>>(); //email -> {offset, length}
        private Dictionary<Tuple<string, string>, string> emailRangeToFileName = new Dictionary<Tuple<string, string>, string>(); // {start,end} -> fileName
        private List<string> emailFilesNames = new List<string>(); //A list with all email file names

        private const long chunckSize = 0x40000000; //1G. 
        private const int emailFileSizeLimit = 0x10000000; //256 MB
        private const int numberOfTokensInLine = 12;
        private int numberOfEmailFiles = 1;   
        
        public BigDataSearcher(string path)
        {
            this.originalFileName = Path.GetFileName(path);
            this.originalFileNameWithoutExt = Path.GetFileNameWithoutExtension(path);
            this.directory = Path.GetDirectoryName(path) + "\\";
            this.FirstPhaseZipCodeIndexFileName = originalFileNameWithoutExt + "_FirstPhaseZipCodeIndexFile.csv";
            this.FirstPhaseEmailIndexFileName = originalFileNameWithoutExt + "_FirstPhaseEmailIndexFile.csv";
            this.SortedFirstPhaseZipCodeIndexFileName = originalFileNameWithoutExt + "_SortedFirstPhaseZipCodeIndexFile.csv";
            this.ZipCodeIndexFileName = originalFileNameWithoutExt + "_ZipCodeIndexFile.csv";
            this.SortedFileName = "Sorted_" + originalFileNameWithoutExt +".csv";
            this.mergedEmailIndicesFileName = "Merged_" + originalFileNameWithoutExt + "_EmailsIndicesFile.csv";
            this.currentEmailIndicesFileInMemory = "";
        }
        
        /// <summary>
        /// Prepare the data base
        /// </summary>
        public void Prepare()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Reset();
                sw.Start();
                CreateIndicesFiles(false);
                sw.Stop();
                System.Console.WriteLine("CreateIndicesFiles - {0}", sw.Elapsed);

                memoryMappedForOriginalFile = MemoryMappedFile.CreateFromFile(directory + "\\" + originalFileName, FileMode.Open, "originalMmf");

                sw.Reset();
                sw.Start();
                SortFirstPhaseZipCodeIndicesFile();
                sw.Stop();
                System.Console.WriteLine("SortFirstPhaseZipCodeIndexFile - {0}", sw.Elapsed);

                SplitEmailIndexFile(sw);

                sw.Reset();
                sw.Start();
                CreateSortedFileByZipCode();
                sw.Stop();
                System.Console.WriteLine("SortOriginalFileByZipCode - {0}", sw.Elapsed);

                sw.Reset();
                sw.Start();
                CreateZipCodeIndicesFileFromSortedFile(false);
                sw.Stop();
                System.Console.WriteLine("CreateZipCodeIndexFileFromSortedFile - {0}", sw.Elapsed);

                memoryMappedForSortedFile = MemoryMappedFile.CreateFromFile(directory + "\\" + SortedFileName, FileMode.Open, "sortedMmf");
            }
            catch (Exception ex)
            {
                //TODO: What shall we do when we were not able to prepare the data?
                System.Console.WriteLine("There was an error while preparing the data. Please try again...");
            }
        }

        /// <summary>
        /// Clean the object
        /// </summary>
        public void Dispose()
        {
            memoryMappedForOriginalFile.Dispose();
            memoryMappedForSortedFile.Dispose();
            zipCodeIndices.Clear();
            emailIndices.Clear();
            emailRangeToFileName.Clear();
            emailFilesNames.Clear();
        }

        /// <summary>
        /// Query by zip code
        /// </summary>
        /// <param name="zipCode"></param>
        /// <returns></returns>
        public QueryResult SearchByZipCode(string zipCode)
        {
            if (string.IsNullOrEmpty(zipCode))
            {
                return new QueryResult { Result = string.Empty, NumberOfRecord = 0};
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();

            //find the zip code in the in-memory dictionary
            Tuple<long, long> zipCodeOffsetAndLength;
            if (!zipCodeIndices.TryGetValue(zipCode, out zipCodeOffsetAndLength))
            {
                sw.Stop();
                //Not found:
                return new QueryResult { Result = string.Empty, NumberOfRecord = 0, ExecTime = sw.Elapsed };
            }

            long offset = zipCodeOffsetAndLength.Item1;
            long length = zipCodeOffsetAndLength.Item2;

            if (length > chunckSize) //corner case. TODO: take care of it.
            {
                sw.Stop();
                return new QueryResult { Result = string.Empty, NumberOfRecord = 0, ExecTime = sw.Elapsed };
            }

            //Read from file:
            string res = ReadFromFile(memoryMappedForSortedFile, offset, length);
            sw.Stop();
            return new QueryResult { Result = res, NumberOfRecord = res.Count(c => c == '\n'), ExecTime = sw.Elapsed };
        }

        /// <summary>
        /// Query by email
        /// </summary>
        /// <param name="email"></param>
        /// <returns></returns>
        public QueryResult SearchByEmail(string email)
        {
            if (string.IsNullOrEmpty(email))
            {
                return new QueryResult { Result = string.Empty, NumberOfRecord = 0 };
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            //1) get the email indices file name that contain the given email
            //2) Load the file to memory if needed
            //3) Find the email's offset and length in the original file
            //4) Open the original file, and get the value

            string emailIndicesFileName = GetEmailIndicesFileName(email);
            if (string.IsNullOrEmpty(emailIndicesFileName))
            {
                sw.Stop();
                return new QueryResult { Result = string.Empty, NumberOfRecord = 0, ExecTime = sw.Elapsed };
            }
            LoadEmailIndicesFileIfNeeded(emailIndicesFileName);

            Tuple<long, long> offsetAndLength = GetEmailOffsetAndLength(email);
            if (offsetAndLength.Item2 == 0)
            {
                //No length to read. Return empty result.
                sw.Stop();
                return new QueryResult { Result = string.Empty, NumberOfRecord = 0, ExecTime = sw.Elapsed };
            }

            string res = ReadFromFile(memoryMappedForOriginalFile, offsetAndLength.Item1, offsetAndLength.Item2);
            sw.Stop();
            return new QueryResult { Result = res, NumberOfRecord = res.Count(c => c == '\n') + 1, ExecTime = sw.Elapsed };
        }


        #region Private Methods

        private void SplitEmailIndexFile(Stopwatch sw)
        {
            sw.Reset();
            sw.Start();
            SortEmailsFiles();
            sw.Stop();
            System.Console.WriteLine("SortEmailsFiles - {0}", sw.Elapsed);

            sw.Reset();
            sw.Start();
            MergeEmailsIndicesFiles();
            sw.Stop();
            System.Console.WriteLine("MergeEmailsIndicesFiles - {0}", sw.Elapsed);

            sw.Reset();
            sw.Start();
            CreateEmailsIndicesFile();
            sw.Stop();
            System.Console.WriteLine("CreateEmailsIndicesFile - {0}", sw.Elapsed);
        }

        /// <summary>
        /// 
        /// Read buffer from file and return it as a string
        /// </summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        private string ReadFromFile(MemoryMappedFile mmf, long offset, long length)
        {
            using (var accessor = mmf.CreateViewStream(offset, length))
            {
                //TODO: There must be a better way to return the buffer.
                byte[] buffer = new byte[length + 1];
                int a = accessor.Read(buffer, 0, (int)length);
                return Encoding.ASCII.GetString(buffer, 0, a);
            }
        }

        /// <summary>
        /// 
        /// Return offset and length in the original file for a given email
        /// </summary>
        /// <param name="email"></param>
        /// <returns></returns>
        private Tuple<long, long> GetEmailOffsetAndLength(string email)
        {
            Tuple<long, long> val;
            if(!emailIndices.TryGetValue(email, out val))
                return new Tuple<long, long>(0, 0);

            return val;
        }

        /// <summary>
        /// Load email indices file if needed. We keep the last loaded file name, so if the given
        /// file name is the same as the last one loaded, do nothing.
        /// </summary>
        /// <param name="emailIndicesFileName"></param>
        private void LoadEmailIndicesFileIfNeeded(string emailIndicesFileName)
        {
            //If it's already in memory -> nothing to do:
            if (emailIndicesFileName.Equals(this.currentEmailIndicesFileInMemory))
                return;

            //Load the file:
            emailIndices.Clear();
            using (FileStream fs = File.Open(emailIndicesFileName, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] tokens = line.Split(',');
                    string email = tokens[0];
                    int offset = int.Parse(tokens[1]);
                    int length = int.Parse(tokens[2]);

                    try
                    {
                        emailIndices.Add(email, new Tuple<long, long>(offset, length));
                    }
                    catch (ArgumentException ex)
                    {
                        //TODO: what should we do if the email is already exist?
                    }
                }
                emailIndicesFileName = this.currentEmailIndicesFileInMemory;
            }
        }

        /// <summary>
        /// Return the file name that contain the given email. If not found, return empty string.
        /// </summary>
        /// <param name="email"></param>
        /// <returns></returns>
        private string GetEmailIndicesFileName(string email)
        {
            foreach (KeyValuePair<Tuple<string, string>, string> kvp in emailRangeToFileName)
            {
                //if the mail is greater than the start and less than the end:
                if ((email.CompareTo(kvp.Key.Item1) >= 0) && (email.CompareTo(kvp.Key.Item2) <= 0))
                {
                    return kvp.Value; //return the file name
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Create zip code indices file from the sorted input file
        /// </summary>
        /// <param name="skipFirstLine"></param>
        private void CreateZipCodeIndicesFileFromSortedFile(bool skipFirstLine)
        {
            //Write the zip code indices file + save it in memory:
            using (FileStream fs = File.Open(directory + "\\" + SortedFileName, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            using (StreamWriter writer = new StreamWriter(directory + "\\" + ZipCodeIndexFileName))
            {
                string line = null;
                if (skipFirstLine)
                {
                    line = sr.ReadLine(); //skip first line
                }
                /// Keep the first Line
                line = sr.ReadLine();
                int pos = 0;
                int length = line.Length + 1;
                string[] tokens = line.Split(',');
                string zipCode = tokens[7].Replace("\"","");
                string currentZipCode = zipCode;

                while ((line = sr.ReadLine()) != null)
                {
                    if (string.IsNullOrEmpty(line))
                        continue;
                    
                    tokens = line.Split(',');
                    zipCode = tokens[7].Replace("\"", "");

                    //If the new zip code is different from the previous one we read,
                    if (! zipCode.Equals(currentZipCode))
                    {
                        //Flush the current zip code
                        writer.WriteLine("{0},{1},{2}", currentZipCode, pos, length);
                        zipCodeIndices.Add(currentZipCode,new Tuple<long,long>(pos,length)); //save the offset+_length
                        pos += length;
                        length = line.Length + 1;
                        currentZipCode = zipCode; //change the current zip code to contain the new one
                    }
                    else
                    {
                        //If the new zip code is exactly as the previous one we read, just increase the length
                        length += line.Length + 1;
                    }
                }

                //Write the last line:
                writer.WriteLine("{0},{1},{2}", currentZipCode, pos, length);//Flush the current zip code
                zipCodeIndices.Add(currentZipCode, new Tuple<long, long>(pos, length));
                fs.Close();
            }
        }

        /// <summary>
        /// Create email & zip-code indices files from the original input file.
        /// </summary>
        /// <param name="skipFirstLine"></param>
        private void CreateIndicesFiles(bool skipFirstLine)
        {
            //We are going to create exactly 1 indices file for zip code and 1 or more indices files for email
            using (FileStream fs = File.Open(directory + "\\" + originalFileName, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            using (StreamWriter zipCodeWriter = new StreamWriter(directory + "\\" + FirstPhaseZipCodeIndexFileName))
            {
                string emailFilePath = directory + "\\" + numberOfEmailFiles.ToString() + "_" + FirstPhaseEmailIndexFileName;
                emailFilesNames.Add(emailFilePath);
                StreamWriter emailWriter = new StreamWriter(emailFilePath);
                int emailCounter = 0;
                int pos = 0;
                string line = null;
                if (skipFirstLine)
                {
                    line = sr.ReadLine(); //skip first line
                    pos = line.Length + 1 ; //+1 for the newline char (CR = 13)
                }
                while ((line = sr.ReadLine()) != null)
                {
                    if (string.IsNullOrEmpty(line)) //ignore empty lines
                        continue;

                    string[] tokens = line.Split(',');
                    if (tokens.Length < numberOfTokensInLine) //igonre incomplete lines
                        continue;

                    //Remove " from strings. TODO: Should we add input validation?
                    string zipCode = tokens[7].Replace("\"", "");
                    string email = tokens[10].Replace("\"", "");
                    
                    //Write the zip code index line:
                    zipCodeWriter.WriteLine("{0},{1},{2}", zipCode, pos, line.Length);

                    //Write the email index line
                    string emailLine = string.Format("{0},{1},{2}", email, pos, line.Length);
                    emailWriter.WriteLine(emailLine);
                    pos += line.Length + 1;
                    emailCounter += emailLine.Length + 1;
                    //Replace email file if needed (i.e. if the we wrote too much):
                    if (emailCounter >= emailFileSizeLimit)
                    {
                        emailWriter.Close();
                        numberOfEmailFiles++;
                        emailFilePath = directory + "\\" + numberOfEmailFiles.ToString() + "_" + FirstPhaseEmailIndexFileName;
                        emailFilesNames.Add(emailFilePath);
                        emailWriter = new StreamWriter(emailFilePath);
                        emailCounter = 0;
                    }
                }
                emailWriter.Close();
                fs.Close();
            }
            //emailFilesNames.ForEach(x => System.Console.WriteLine(x));

        }

        /// <summary>
        /// Sort the first phase zip code indices file
        /// We sort it in memory. 
        /// TODO: replace this sorting technique with many files, merge, sort and split technique like for emails
        /// </summary>
        private void SortFirstPhaseZipCodeIndicesFile()
        {
            SortedDictionary<string, List<Tuple<long, long>>> dic = new SortedDictionary<string, List<Tuple<long, long>>>();
            using (FileStream fs = File.Open(directory + "\\" + FirstPhaseZipCodeIndexFileName, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] tokens = line.Split(',');
                    string zipCode = tokens[0];
                    int offset = int.Parse(tokens[1]);
                    int length = int.Parse(tokens[2]);

                    //For every zip code create a list with its offset+length in the sorted input file
                    List<Tuple<long, long>> ltll;
                    if (dic.TryGetValue(zipCode, out ltll))
                    {
                        ltll.Add(new Tuple<long, long>(offset, length));
                    }
                    else
                    {
                        ltll = new List<Tuple<long, long>> { new Tuple<long, long>(offset, length) };
                        dic.Add(zipCode, ltll);
                    }
                    //System.Console.WriteLine("zipCode= {0}, offset={1}, length={2}", zipCode, offset, length);
                }
            }

            //Flush the dictionary to file. For every zip code write all its occurrences together in a row.
            using (StreamWriter writer = new StreamWriter(directory + "\\" + SortedFirstPhaseZipCodeIndexFileName))
            {
                foreach (KeyValuePair<string, List<Tuple<long,long>>> pair in dic)
                {
                    foreach (Tuple<long,long> bf1 in pair.Value)
                    {
                        writer.WriteLine("{0},{1},{2}", pair.Key, bf1.Item1, bf1.Item2);
                    }
                }
            }
        }

        /// <summary>
        /// Sort every email file
        /// </summary>
        private void SortEmailsFiles()
        {
            //Read every email files into a sorted dictionary, and then write the sorted dic to the same file
            emailFilesNames.ForEach(x => SortEmailFile(x));
        }

        /// <summary>
        /// Sort an email file
        /// </summary>
        /// <param name="emailFilePath"></param>
        private void SortEmailFile(string emailFilePath)
        {
            //Load the file to memory
            //Keep it in a dictionary
            SortedDictionary<string, Tuple<long, long>> dic = new SortedDictionary<string, Tuple<long, long>>();
            using (FileStream fs = File.Open(emailFilePath, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] tokens = line.Split(',');
                    string email = tokens[0];
                    int offset = int.Parse(tokens[1]);
                    int length = int.Parse(tokens[2]);

                    //For every email keep its offset+length in the original file
                    Tuple<long, long> val;
                    if (dic.TryGetValue(email, out val))
                    {
                        //TODO: What should we do if there is a duplicated email???
                    }
                    else
                    {
                        dic.Add(email, new Tuple<long, long>(offset, length));
                    }
                    //System.Console.WriteLine("zipCode= {0}, offset={1}, length={2}", zipCode, offset, length);
                }
                fs.Close();
            }

            //Open the same file and write the sorted values:
            using (StreamWriter writer = new StreamWriter(emailFilePath))
            {
                foreach (KeyValuePair<string, Tuple<long, long>> pair in dic)
                {
                    writer.WriteLine("{0},{1},{2}", pair.Key, pair.Value.Item1, pair.Value.Item2);
                }
            }
        }

        /// <summary>
        /// Merge all email indices files into one file
        /// </summary>
        private void MergeEmailsIndicesFiles()
        {
            using (StreamWriter writer = new StreamWriter(directory + "\\" + mergedEmailIndicesFileName))
            {
                //1) Init array of readers and array of strings
                //2) Read next line from All Files into the strings array. 
                //  2.1) If all strings are null, break the loop
                //3) Find the index (i) of the smallest string
                //4) Get the string from position i, replace it with null
                //5) Write the string to the merged file
                //6) Loop

                //Init readers:
                StreamReader[] readers = new StreamReader[emailFilesNames.Count];
                string[] nextLines = new string[emailFilesNames.Count];
                initReaders(readers);
                ReadNextLines(readers, nextLines);
                while (ThereAreMoreLines(nextLines))
                {
                    int i = GetIndexOfTheSmallestEmail(nextLines);
                    string nextLine = nextLines[i];
                    writer.WriteLine(nextLine);
                    nextLines[i] = null;
                    ReadNextLines(readers, nextLines);
                }

                foreach (StreamReader r in readers)
                {
                    r.Close();
                }            
            }
            //Last step: delete all intermediate email files:
            emailFilesNames.ForEach(x => File.Delete(x));
            numberOfEmailFiles = 1;
        }

        /// <summary>
        /// Create emails indices files from the merged file. 
        /// Split the merged file into many files with a limited size .
        /// </summary>
        private void CreateEmailsIndicesFile()
        {
            //Split the emails Indices files into many files. Keep first and last values for each file
            //It will be used later to find the correct file while querying by email
            using (FileStream fs = File.Open(directory + "\\" + mergedEmailIndicesFileName, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            {
                string emailFilePath = directory + "\\" + numberOfEmailFiles.ToString() + "_" + FirstPhaseEmailIndexFileName;
                emailFilesNames.Add(emailFilePath);
                StreamWriter emailWriter = new StreamWriter(emailFilePath);
                int emailLengthCounter = 0;
                string startEmail = "";
                string email = "";
                string line = null;
                while ((line = sr.ReadLine()) != null)
                {
                    if (string.IsNullOrEmpty(line))
                        continue;
                    string[] tokens = line.Split(',');
                    email = tokens[0];

                    //Keep the start email:
                    if (emailLengthCounter == 0)
                        startEmail = email;
                    
                    emailWriter.WriteLine(line);
                    emailLengthCounter += line.Length + 1;
                    
                    //Replace email file if needed:
                    if (emailLengthCounter >= emailFileSizeLimit)
                    {
                        emailRangeToFileName.Add(new Tuple<string, string>(startEmail, email), emailFilePath); //keep a map of start & end emails in a file to file path

                        emailWriter.Close();
                        numberOfEmailFiles++;
                        emailFilePath = directory + "\\" + numberOfEmailFiles.ToString() + "_" + FirstPhaseEmailIndexFileName;
                        emailFilesNames.Add(emailFilePath); //keep the file name
                        
                        emailWriter = new StreamWriter(emailFilePath);
                        emailLengthCounter = 0;
                        startEmail = "";
                    }
                }
                //Add the last one:
                emailRangeToFileName.Add(new Tuple<string, string>(startEmail, email), emailFilePath); //keep a map of start & end emails in a file to file path
                emailFilesNames.Add(emailFilePath); //keep the file name
                numberOfEmailFiles++;

                emailWriter.Close();
                fs.Close();
            }
        }

        /// <summary>
        /// Find the index in the given array of the smallest email string.
        /// </summary>
        /// <param name="nextLines"></param>
        /// <returns></returns>
        private int GetIndexOfTheSmallestEmail(string[] nextLines)
        {
            //Find the first string that is not empty:
            string currentString = nextLines.First(x => !string.IsNullOrEmpty(x));
            int index = 0;
            for (int i = 0; i < nextLines.Length; i++)
            {
                //If we find non empty string that is less than the current string
                if ((!string.IsNullOrEmpty(nextLines[i]) && (nextLines[i].CompareTo(currentString) <= 0)))
                    index = i;
            }
            return index;
        }

        /// <summary>
        /// Return whether there are more non empty or null strings in the array
        /// </summary>
        /// <param name="nextLines"></param>
        /// <returns></returns>
        private bool ThereAreMoreLines(string[] nextLines)
        {
            for (int i = 0; i < nextLines.Length; i++)
            {
                if (!string.IsNullOrEmpty(nextLines[i]))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Read the next lines from the given readers array into the strings array, 
        /// only if the string in the strings array is not null/empty
        /// </summary>
        /// <param name="readers"></param>
        /// <param name="nextLines"></param>
        private void ReadNextLines(StreamReader[] readers, string[] nextLines)
        {
            
            for (int i = 0; i < nextLines.Length; i++)
            {
                if (string.IsNullOrEmpty(nextLines[i]))
                {
                    nextLines[i] = readers[i].ReadLine();
                }
            }
        }

        /// <summary>
        /// Init the readers based on all email file names
        /// </summary>
        /// <param name="readers"></param>
        private void initReaders(StreamReader[] readers)
        {
            int i =0;
            foreach (string fileName in emailFilesNames)
            {
                FileStream fs = File.Open(fileName, FileMode.Open, FileAccess.Read);
                StreamReader sr = new StreamReader(fs);
                readers[i] = sr;
                i++;
            }
        }

        /// <summary>
        /// Create the sorted input file, based on the zip code indices file
        /// </summary>
        private  void CreateSortedFileByZipCode()
        {
            FileInfo f = new FileInfo(directory + "\\" + originalFileName);
            long sizeOfFile = f.Length;
            using (FileStream fs = File.Open(directory + "\\" + SortedFirstPhaseZipCodeIndexFileName, FileMode.Open, FileAccess.Read))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            using (StreamWriter writer = new StreamWriter(directory + "\\" + SortedFileName))
            {
                long startOfStreamInFile = 0;
                long l = chunckSize < sizeOfFile ? chunckSize : sizeOfFile;
                MemoryMappedViewStream accessor = memoryMappedForOriginalFile.CreateViewStream(startOfStreamInFile, l);

                string line;// = sr.ReadLine(); //skip first line
                while ((line = sr.ReadLine()) != null)
                {
                    string[] tokens = line.Split(',');
                    string zipCode = tokens[0];
                    int offsetInFile = int.Parse(tokens[1]);
                    int length = int.Parse(tokens[2]);

                    //Optimization: calculate if the next read is within the current stream we have.
                    //If not, load a new stream with big chunck
                    if (!IsNextReadInCurrentStream(accessor, length, offsetInFile, startOfStreamInFile))
                    {
                        LoadNewStream(ref startOfStreamInFile, ref accessor, length, memoryMappedForOriginalFile, sizeOfFile, offsetInFile);
                    }
                    //Set the position where to start read:
                    accessor.Position = offsetInFile - startOfStreamInFile;
                    byte[] buffer = new byte[length + 1];
                    int a = accessor.Read(buffer, 0, length);
                    //System.Console.WriteLine("buffer= {0}", Encoding.ASCII.GetString(buffer, 0, a));
                    writer.Write("{0}\n", Encoding.ASCII.GetString(buffer, 0, a));
                }
                writer.Close();
            }
        }

        /// <summary>
        /// Return whether the next chunck to read is within the current stream. For optimization.
        /// </summary>
        /// <param name="accessor"></param>
        /// <param name="length"></param>
        /// <param name="offsetInFile"></param>
        /// <param name="startOfStreamInFile"></param>
        /// <returns></returns>
        private bool IsNextReadInCurrentStream(MemoryMappedViewStream accessor, int length, int offsetInFile, long startOfStreamInFile)
        {
            return (offsetInFile >= startOfStreamInFile) && (offsetInFile + length < startOfStreamInFile + accessor.Length);
        }

        /// <summary>
        /// Load new stream from file starting at the given offset
        /// </summary>
        /// <param name="startOfStream"></param>
        /// <param name="accessor"></param>
        /// <param name="length"></param>
        /// <param name="mmf"></param>
        /// <param name="sizeOfFile"></param>
        /// <param name="offsetInFile"></param>
        private void LoadNewStream(ref long startOfStream, ref MemoryMappedViewStream accessor, int length, MemoryMappedFile mmf, long sizeOfFile, long offsetInFile)
        {
            long newStartOfStream = offsetInFile;
            long endPoint = newStartOfStream + length + chunckSize;
            long newLength = (endPoint > sizeOfFile) ? length : length + chunckSize;
            newLength = (newLength > chunckSize) ? chunckSize : newLength; //Do not load streams more than chunck
            accessor.Dispose();
            accessor = mmf.CreateViewStream(newStartOfStream, newLength);
            startOfStream = newStartOfStream;
        }

        /// <summary>
        /// Return whether there is enough stream to read. For optimization.
        /// </summary>
        /// <param name="startOfStreamInFile"></param>
        /// <param name="accessor"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        private bool ThereIsEnoughStream(long startOfStreamInFile, MemoryMappedViewStream accessor, int length)
        {
            return startOfStreamInFile + accessor.Length - accessor.Position > length;
        }

        #endregion

        
    }
}
