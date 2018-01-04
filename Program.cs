using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
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
                int i = 0;

                using (var stream = response.GetResponseStream())
                using (var gzipstream = new GZipStream(stream, CompressionMode.Decompress))
                using (StreamReader reader = new StreamReader(gzipstream))
                {
                    var headers = reader.ReadLine().Split(new[] { ';' });
                    Console.WriteLine(String.Join(" ", headers));

                    while(!reader.EndOfStream && i++ < 2)
                    {
                        var values = reader.ReadLine().Split(new[] { ';' });

                        var enhet = headers.Zip(values, (header, value) => new { header, value} )
                                            .ToDictionary(item => item.header.Replace(@"""", ""), item => (object)item.value);

                    	dynamic expando = new ExpandoObject();
	                    var expandoDic = (IDictionary<string, object>)expando;

                        foreach (var kvp in enhet)
                        {
                            expandoDic.Add(kvp);
                            Console.WriteLine(kvp);
                        }

                        Console.WriteLine(expando.navn);
                        Console.WriteLine(expandoDic.ContainsKey(@"""organisasjonsnummer"""));
                    }
                }
            }            
        }
    }
}

