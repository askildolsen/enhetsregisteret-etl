using System;
using System.Collections.Generic;
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
            public string overordnetEnhet { get; set; }
        }

        public class Enhet
        {
            public string Organisasjonsnummer { get; set; }
            public string Navn { get; set; }

            public IEnumerable<Enhet> Underenheter { get; set; }
        }

        public EnhetsregisteretIndex()
        {
            AddMap<Enhetsregisteret>(enheter =>
                from enhet in enheter
                where String.IsNullOrEmpty(enhet.overordnetEnhet)
                select new Enhet
                {
                    Organisasjonsnummer = enhet.organisasjonsnummer,
                    Navn = enhet.navn,
                    Underenheter = new Enhet[] { }
                }
            );

            AddMap<Enhetsregisteret>(enheter =>
                from underenhet in enheter
                where !String.IsNullOrEmpty(underenhet.overordnetEnhet)
                select new Enhet
                {
                    Organisasjonsnummer = underenhet.overordnetEnhet,
                    Navn = null,
                    Underenheter = new Enhet[] {
                        new Enhet {
                            Organisasjonsnummer = underenhet.organisasjonsnummer,
                            Navn = underenhet.navn                           
                        }
                     }
                }
            );

            Reduce = results  =>
                from result in results
                group result by result.Organisasjonsnummer into g
                select new
                {
                    Organisasjonsnummer = g.Key,
                    Navn = g.FirstOrDefault(enhet => enhet.Navn != null).Navn,
                    Underenheter = g.SelectMany(enhet => enhet.Underenheter)
                };

            //OutputReduceToCollection = "Enhet";
        }
    }
}
