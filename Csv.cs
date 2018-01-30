using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
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
                    var headers = reader.ReadLine().Split(new[] { ';' });

                    var store = DocumentStoreHolder.Store;
                    using (BulkInsertOperation bulkInsert = store.BulkInsert())
                    {
                        while(!reader.EndOfStream)
                        {
                            var values = reader.ReadLine().Split(new[] { ';' });

                            var enhet = headers.Zip(values, (header, value) => new { header, value} )
                                                .ToDictionary(item => item.header.Trim('"'), item => (object)item.value.Trim('"'));

                            dynamic expando = new ExpandoObject();
                            var expandoDic = (IDictionary<string, object>)expando;

                            foreach (var kvp in enhet)
                            {
                                expandoDic.Add(kvp);
                            }

                            yield return expando;
                        }
                    }
                }
            }
        }
    }
}
