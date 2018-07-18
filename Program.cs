﻿using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Xml;
using System.Xml.Linq;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Json;
using static MoreLinq.Extensions.BatchExtension;

namespace enhetsregisteret_etl
{
    class Program
    {
        static void Main(string[] args)
        {
            new Enhetsregisteret.EnhetsregisteretIndex().Execute(DocumentStoreHolder.Store);
            new Enhetsregisteret.EnhetsRegisteretResourceModel.EnhetsregisteretResourceIndex().Execute(DocumentStoreHolder.Store);

            foreach (var batch in Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/enheter")).Batch(10000))
            {
                using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
                {
                    foreach (dynamic enhet in batch)
                    {
                        Console.Write(enhet.organisasjonsnummer + " ");
                        bulkInsert.Store(
                            enhet,
                            "Enhetsregisteret/" + enhet.organisasjonsnummer,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
                        );
                    }
                }
            }

            foreach (var batch in Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/underenheter")).Batch(10000))
            {
                using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
                {
                    foreach (dynamic underenhet in batch)
                    {
                        Console.Write(underenhet.organisasjonsnummer + " ");
                        bulkInsert.Store(
                            underenhet,
                            "Enhetsregisteret/" + underenhet.organisasjonsnummer,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
                        );
                    }
                }
            }

            using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
            {
                foreach (dynamic frivillig in Csv.ExpandoStream(WebRequest.Create("http://hotell.difi.no/download/brreg/frivillighetsregisteret")))
                {
                    bulkInsert.Store(
                        frivillig,
                        "Frivillighetsregisteret/" + frivillig.orgnr,
                        new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Frivillighetsregisteret"}})
                    );
                }
            }

            using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
            {
                foreach (dynamic stotte in Xml.ExpandoStream(WebRequest.Create("https://data.brreg.no/rofs/od/rofs/stottetildeling/nob")))
                {
                    bulkInsert.Store(
                        stotte,
                        "Stotteregisteret/" + stotte.tildelingId,
                        new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Stotteregisteret"}})
                    );
                }
            }
        }
    }
}
