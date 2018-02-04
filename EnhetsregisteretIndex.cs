using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;

namespace Enhetsregisteret
{
    public class EnhetsregisteretIndex : AbstractMultiMapIndexCreationTask<EnhetsregisteretIndex.Enhet>
    {
        public class Enhetsregisteret {
            public string organisasjonsnummer { get; set; }
            public string navn { get; set; }
        }

        public class Enhet {
            public string Organisasjonsnummer { get; set; }
            public string Navn { get; set; }
        }

        public EnhetsregisteretIndex()
        {
            AddMap<Enhetsregisteret>(enheter =>
                from enhet in enheter
                select new
                {
                    Organisasjonsnummer = enhet.organisasjonsnummer,
                    Navn = enhet.navn
                }
            );

            Reduce = results  =>
                from result in results
                group result by result.Organisasjonsnummer into g
                select new
                {
                    Organisasjonsnummer = g.Key,
                    Navn = g.First().Navn
                };

            //OutputReduceToCollection = "Enhet";
        }
    }
}
