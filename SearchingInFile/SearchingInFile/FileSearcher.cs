using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace SearchingInFile
{
    public class FileSearcher
    {
        public FileSearcher(string filePath)
	    {
            this._filePath = filePath;
            this.ErrorMessage = "";
            this._nameToIds = new Dictionary<string, List<int>>();
            this._emailToIds = new Dictionary<string, List<int>>();
	    }

        public string ErrorMessage { get; private set; }

        public List<int> Search(string str)
        {
            if (string.IsNullOrEmpty(str))
                return new List<int>();

            if (isEmail(str))
                return search(str, this._emailToIds);

            return search(str, this._nameToIds);
        }

        public bool Init()
        {
            try
            {
                using (var sr = new StreamReader(_filePath))
                {
                    while (!sr.EndOfStream)
                    {
                        var splits = sr.ReadLine().Split(',');
                        int id = int.Parse(splits[0].Trim());
                        string name = splits[1].Trim();
                        string email = splits[2].Trim();

                        add(name, id, _nameToIds);
                        add(email, id, _emailToIds);
                    }
                }
            }
            catch (Exception e)
            {
                ErrorMessage = "File cannot be opened";
                return false;                
            }

            return true;
            
        }


        #region Private Methods 

        private List<int> search(string str, Dictionary<string, List<int>> container)
        {
            List<int> result;
            if (container.TryGetValue(str, out result))
                return result;

            return new List<int>();
        }

        private bool isEmail(string str)
        {
            return str.Contains("@");
        }

        private void add(string str, int id, Dictionary<string, List<int>> container)
        {
            List<int> ids;
            if (container.TryGetValue(str, out ids))
            {
                //if the given string (name or email) is already in the container, 
                //just add the new id to its list:
                ids.Add(id);
            }
            else
            {
                //Otherwise, create a new list with the given id:
                container.Add(str, new List<int>() { id });
            }
        }

        #endregion

        ///
        /// Private Fields
        ///
        private string _filePath;
        private Dictionary<string, List<int>> _nameToIds;
        private Dictionary<string, List<int>> _emailToIds;
    }
}
