using System;
using System.IO;
using System.IO.Compression;
using System.Net;

namespace enhetsregisteret_etl
{
    class Program
    {
        static void Main(string[] args)
        {
            WebRequest request = WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/enheter");
            using (WebResponse response = request.GetResponse())
            {
                using (var stream = response.GetResponseStream())
                using (var gzipstream = new GZipStream(stream, CompressionMode.Decompress))
                using (StreamReader reader = new StreamReader(gzipstream))
                {
                    while(!reader.EndOfStream)
                    {
                        var ret = reader.ReadLine();
                        Console.WriteLine(ret);
                    }
                }
            }            
        }
    }
}

