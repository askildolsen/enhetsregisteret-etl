using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

namespace enhetsregisteret_etl
{
    class Csv
    {
        public static IEnumerable<ExpandoObject> ExpandoStream(WebRequest request)
        {
            using (WebResponse response = request.GetResponse())
            using (var stream = response.GetResponseStream())
            using (StreamReader reader = new StreamReader(stream, Encoding.GetEncoding(((HttpWebResponse)response).CharacterSet)))
            {
                foreach(var expando in ParseStream(reader))
                {
                    yield return expando;
                }
            }
        }

        public static IEnumerable<ExpandoObject> ExpandoStreamGZip(WebRequest request)
        {
            using (WebResponse response = request.GetResponse())
            using (var stream = response.GetResponseStream())
            using (var gzipstream = new GZipStream(stream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(gzipstream))
            {
                foreach(var expando in ParseStream(reader))
                {
                    yield return expando;
                }
            }
        }

        private static IEnumerable<ExpandoObject> ParseStream(StreamReader reader)
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
