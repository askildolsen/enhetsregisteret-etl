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
    class Program
    {
        static void Main(string[] args)
        {
            new Enhetsregisteret.EnhetsregisteretIndex().Execute(DocumentStoreHolder.Store);

            using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
            {
                foreach (dynamic enhet in Csv.ExpandoStream(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/enheter")))
                {
                    Console.WriteLine(enhet.navn);
                    bulkInsert.Store(
                        enhet,
                        "Enhetsregisteret/" + enhet.organisasjonsnummer,
                        new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
                    );                    
                }
            }
        }
    }
}
