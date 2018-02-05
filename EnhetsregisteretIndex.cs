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
            public string postadresse_adresse { get; set; }
            public string postadresse_postnummer { get; set; }
            public string postadresse_poststed { get; set; }
            public string forretningsadresse_adresse { get; set; }
            public string forretningsadresse_postnummer { get; set; }
            public string forretningsadresse_poststed { get; set; }            
        }

        public class Enhet
        {
            public string Organisasjonsnummer { get; set; }
            public string Navn { get; set; }
            public GeografiskAdresse Postadresse { get; set; }
            public GeografiskAdresse Forretningsadresse { get; set; }
            public IEnumerable<Enhet> Underenheter { get; set; }
        }

        public class GeografiskAdresse
        {
            public string Adresse { get; set; }
            public string Postnummer { get; set; }
            public string PostSted { get; set; }
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
                    Postadresse = (String.IsNullOrEmpty(enhet.postadresse_postnummer)) ? null :
                        new GeografiskAdresse
                        {
                            Adresse = enhet.postadresse_adresse,
                            Postnummer = enhet.postadresse_postnummer,
                            PostSted = enhet.postadresse_poststed
                        },
                    Forretningsadresse = (String.IsNullOrEmpty(enhet.forretningsadresse_postnummer)) ? null :
                        new GeografiskAdresse
                        {
                            Adresse = enhet.forretningsadresse_adresse,
                            Postnummer = enhet.forretningsadresse_postnummer,
                            PostSted = enhet.forretningsadresse_poststed
                        },                        
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
                    Postadresse = null,
                    Forretningsadresse = null,
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
                select new Enhet
                {
                    Organisasjonsnummer = g.Key,
                    Navn = g.Select(enhet => enhet.Navn).FirstOrDefault(navn => navn != null),
                    Postadresse = g.Select(enhet => enhet.Postadresse).FirstOrDefault(adresse => adresse != null),
                    Forretningsadresse = g.Select(enhet => enhet.Forretningsadresse).FirstOrDefault(adresse => adresse != null),
                    Underenheter = g.SelectMany(enhet => enhet.Underenheter)
                };

            //OutputReduceToCollection = "Enhet";
        }
    }
}
