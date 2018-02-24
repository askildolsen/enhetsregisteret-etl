using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Json;

namespace enhetsregisteret_etl
{
    class Csv
    {
        public static IEnumerable<ExpandoObject> ExpandoStream(WebRequest request)
        {
            using (WebResponse response = request.GetResponse())
            {
                using (var stream = response.GetResponseStream())
                using (var gzipstream = new GZipStream(stream, CompressionMode.Decompress))
                using (StreamReader reader = new StreamReader(gzipstream))
                {
                    var CSVParser = new Regex(@"(""([^""]*)""|[^;]*)(;|$)", RegexOptions.Compiled);
                    var headers = CSVParser.Matches(reader.ReadLine()).Select(m => m.Value.Trim(';').Trim('"'));

                    while(!reader.EndOfStream)
                    {
                        dynamic expando = new ExpandoObject();
                        var expandoDic = (IDictionary<string, object>)expando;

                        var values = CSVParser.Matches(reader.ReadLine()).Select(m => m.Value.Trim(';').Trim('"'));

                        foreach (var kvp in headers.Zip(values, (header, value) => new { header, value } )
                            .Where(item => !String.IsNullOrWhiteSpace(item.value)))
                        {
                            expandoDic.Add(kvp.header, kvp.value);
                        }

                        yield return expando;
                    }
                }
            }
        }
    }
}
