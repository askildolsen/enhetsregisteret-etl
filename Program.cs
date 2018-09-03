using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
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

            new Enhetsregisteret.EnhetsRegisteretResourceModel.EnhetsregisteretResourceIndex().Execute(DocumentStoreHolder.Store);

            Func<dynamic, dynamic> enhetmap = e => new {
                    entity = e,
                    id = "Enhetsregisteret/" + e.organisasjonsnummer,
                    meta = new MetadataAsDictionary(new Dictionary<string, object> {{ "@collection", "Enhetsregisteret"}})
            };

            var buffer = new BufferBlock<IEnumerable<ExpandoObject>>(new DataflowBlockOptions { BoundedCapacity = 2 });

            var consumer1 = ConsumeAsync(buffer, enhetmap);
            var consumer2 = ConsumeAsync(buffer, enhetmap);

            await Task.WhenAll(
                ProduceAsync(buffer, () => Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/enheter"))),
                ProduceAsync(buffer, () => Csv.ExpandoStreamGZip(WebRequest.Create("http://data.brreg.no/enhetsregisteret/download/underenheter")))
            );

            buffer.Complete();

            await Task.WhenAll(consumer1, consumer2);

            Console.WriteLine(" Enheter: {0}", sw.Elapsed);

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

        static async Task ProduceAsync(BufferBlock<IEnumerable<ExpandoObject>> queue, Func<IEnumerable<ExpandoObject>> produce)
        {
            try
            {
                foreach (var batch in produce().Batch(10000))
                {
                    await queue.SendAsync(batch);
                    Console.Write("+");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                Console.Write("Produce avslutter");
            }
        }

        static async Task ConsumeAsync(IReceivableSourceBlock<IEnumerable<ExpandoObject>> source, Func<dynamic, dynamic> map)
        {
            while (await source.OutputAvailableAsync())
            {
                IEnumerable<ExpandoObject> batch;
                
                while(source.TryReceive(out batch))
                {
                    using (BulkInsertOperation bulkInsert = DocumentStoreHolder.Store.BulkInsert())
                    {
                        foreach (dynamic e in batch.Select(e => map(e)))
                        {
                            await bulkInsert.StoreAsync(
                                e.entity,
                                e.id,
                                e.meta
                            );
                        }
                    }
                    Console.Write("-");
                }
            }
        }
    }
}
