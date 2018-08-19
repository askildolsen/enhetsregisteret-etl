using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Json;
using static MoreLinq.Extensions.BatchExtension;

namespace enhetsregisteret_etl
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var sw = Stopwatch.StartNew();

            new Enhetsregisteret.EnhetsregisteretIndex().Execute(DocumentStoreHolder.Store);
            new Enhetsregisteret.EnhetsRegisteretResourceModel.EnhetsregisteretResourceIndex().Execute(DocumentStoreHolder.Store);

            foreach (var batch in Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/enheter")).Batch(10000))
            {
                using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
                {
                    foreach (dynamic enhet in batch)
                    {
                        await bulkInsert.StoreAsync(
                            enhet,
                            "Enhetsregisteret/" + enhet.organisasjonsnummer,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
                        );
                    }
                }
                Console.Write(".");
            }

            Console.WriteLine(" Enheter: {0}", sw.Elapsed);

            foreach (var batch in Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/underenheter")).Batch(10000))
            {
                using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
                {
                    foreach (dynamic underenhet in batch)
                    {
                        await bulkInsert.StoreAsync(
                            underenhet,
                            "Enhetsregisteret/" + underenhet.organisasjonsnummer,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
                        );
                    }
                }
                Console.Write(".");
            }

            Console.WriteLine(" Underenheter: {0}", sw.Elapsed);

            using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
            {
                foreach (dynamic frivillig in Csv.ExpandoStream(WebRequest.Create("http://hotell.difi.no/download/brreg/frivillighetsregisteret")))
                {
                    await bulkInsert.StoreAsync(
                        frivillig,
                        "Frivillighetsregisteret/" + frivillig.orgnr,
                        new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Frivillighetsregisteret"}})
                    );
                }
            }

            Console.WriteLine(". Frivillighetsregisteret: {0}", sw.Elapsed);

            using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
            {
                foreach (dynamic stotte in Xml.ExpandoStream(WebRequest.Create("https://data.brreg.no/rofs/od/rofs/stottetildeling/nob")))
                {
                    await bulkInsert.StoreAsync(
                        stotte,
                        "Stotteregisteret/" + stotte.tildelingId,
                        new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Stotteregisteret"}})
                    );
                }
            }

            Console.WriteLine(". Stotteregisteret: {0}", sw.Elapsed);
        }
    }
}
