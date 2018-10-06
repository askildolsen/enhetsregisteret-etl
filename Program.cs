using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Net;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Json;

namespace enhetsregisteret_etl
{
    class Program
    {
        static void Main(string[] args)
        {
            var sw = Stopwatch.StartNew();

            new Enhetsregisteret.EnhetsRegisteretResourceModel.EnhetsregisteretResourceIndex().Execute(DocumentStoreHolder.Store);

            var bulkInsertEnheter = new ActionBlock<ExpandoObject[]>(batch =>
                {
                    using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
                    {
                        foreach (dynamic e in batch)
                        {
                            bulkInsert.Store(
                                e,
                                "Enhetsregisteret/" + e.organisasjonsnummer,
                                new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
                            );
                        }
                    }

                    Console.Write(".");
                },
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 2 }
            );

            var batchEnheter = new BatchBlock<ExpandoObject>(10000, new GroupingDataflowBlockOptions { BoundedCapacity = 10000 });
            batchEnheter.LinkTo(bulkInsertEnheter, new DataflowLinkOptions { PropagateCompletion = true});

            Parallel.ForEach(new[] { "enheter", "underenheter"}, (dataset) =>
            {
                foreach (ExpandoObject e in Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/" + dataset)))
                {
                    batchEnheter.Post(e);
                }
                Console.Write(" {0} {1} lest ", sw.Elapsed, dataset);
            });

            batchEnheter.Complete();
            bulkInsertEnheter.Completion.Wait();

            Console.WriteLine(" Enheter: {0}", sw.Elapsed);

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

            Console.WriteLine(". Frivillighetsregisteret: {0}", sw.Elapsed);

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

            Console.WriteLine(". Stotteregisteret: {0}", sw.Elapsed);
        }
    }
}
