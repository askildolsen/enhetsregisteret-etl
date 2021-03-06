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
            using (var store = new DocumentStore { Urls = new string[] { "http://localhost:8080" }, Database = "Digitalisert" })
            {
                store.Conventions.FindCollectionName = t => t.Name;
                store.Initialize();

                var sw = Stopwatch.StartNew();

                using (BulkInsertOperation bulkInsert = store.BulkInsert())
                {
                    foreach (dynamic organisasjonsform in Csv.ExpandoStream(WebRequest.Create("https://data.ssb.no/api/klass/v1//versions/578.csv?language=nb")))
                    {
                        bulkInsert.Store(
                            organisasjonsform,
                            "Enheter/Organisasjonsform/" + organisasjonsform.code,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enheter"}})
                        );
                    }

                    foreach (dynamic naeringskode in Csv.ExpandoStream(WebRequest.Create("https://data.ssb.no/api/klass/v1//versions/30.csv?language=nb")))
                    {
                        bulkInsert.Store(
                            naeringskode,
                            "Enheter/Naeringskode/" + naeringskode.code,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enheter"}})
                        );
                    }

                    foreach (dynamic sektorkode in Csv.ExpandoStream(WebRequest.Create("https://data.ssb.no/api/klass/v1//versions/92.csv?language=nb")))
                    {
                        bulkInsert.Store(
                            sektorkode,
                            "Enheter/Sektorkode/" + sektorkode.code,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enheter"}})
                        );
                    }
                }

                var bulkInsertEnheter = new ActionBlock<ExpandoObject[]>(batch =>
                    {
                        using (BulkInsertOperation bulkInsert = store.BulkInsert())
                        {
                            foreach (dynamic e in batch)
                            {
                                bulkInsert.Store(
                                    e,
                                    "Enheter/" + e.orgnr,
                                    new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enheter"}})
                                );
                            }
                        }

                        Console.Write(".");
                    },
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 2 }
                );

                var batchEnheter = new BatchBlock<ExpandoObject>(10000, new GroupingDataflowBlockOptions { BoundedCapacity = 10000 });
                batchEnheter.LinkTo(bulkInsertEnheter, new DataflowLinkOptions { PropagateCompletion = true});

                Parallel.ForEach(new[] { "enhetsregisteret", "underenheter"}, (dataset) =>
                {
                    foreach (ExpandoObject e in Csv.ExpandoStream(WebRequest.Create("http://hotell.difi.no/download/brreg/" + dataset)))
                    {
                        batchEnheter.Post(e);
                    }
                    Console.Write(" {0} {1} lest ", sw.Elapsed, dataset);
                });

                batchEnheter.Complete();
                bulkInsertEnheter.Completion.Wait();

                Console.WriteLine(" Enheter: {0}", sw.Elapsed);

                using (BulkInsertOperation bulkInsert = store.BulkInsert())
                {
                    foreach (dynamic frivillig in Csv.ExpandoStream(WebRequest.Create("http://hotell.difi.no/download/brreg/frivillighetsregisteret")))
                    {
                        bulkInsert.Store(
                            frivillig,
                            "Enheter/Frivillighetsregisteret/" + frivillig.orgnr,
                            new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enheter"}})
                        );
                    }
                }

                Console.WriteLine(". Frivillighetsregisteret: {0}", sw.Elapsed);

                using (BulkInsertOperation bulkInsert = store.BulkInsert())
                {
                    foreach (dynamic stotte in Xml.ExpandoStream(WebRequest.Create("https://data.brreg.no/rofs/od/rofs/stottetildeling/nob")))
                    {
                        bulkInsert.Store(
                            stotte,
                            "Enheter/Stotteregisteret/" + stotte.tildelingId,
                            new MetadataAsDictionary(new Dictionary<string, object> { { "@collection", "Enheter"}})
                        );
                    }
                }

                Console.WriteLine(". Stotteregisteret: {0}", sw.Elapsed);

                new EnheterResourceModel.EnheterResourceIndex().Execute(store);
            }
        }
    }
}
