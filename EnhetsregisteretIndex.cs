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
            public string beliggenhetsadresse_adresse { get; set; }
            public string beliggenhetsadresse_postnummer { get; set; }
            public string beliggenhetsadresse_poststed { get; set; }            
            public string naeringskode1_kode { get; set; }
            public string naeringskode1_beskrivelse { get; set; }
            public string naeringskode2_kode { get; set; }
            public string naeringskode2_beskrivelse { get; set; }
            public string naeringskode3_kode { get; set; }
            public string naeringskode3_beskrivelse { get; set; }
            public string orgform_kode { get; set; }
            public string orgform_beskrivelse { get; set; }
        }

        public class Enhet
        {
            public string Organisasjonsnummer { get; set; }
            public string Navn { get; set; }
            public KodeListe Organisasjonsform { get; set; }
            public IEnumerable<KodeListe> Naeringskoder { get; set; }
            public GeografiskAdresse Postadresse { get; set; }
            public GeografiskAdresse Forretningsadresse { get; set; }
            public GeografiskAdresse Beliggenhetsadresse { get; set; }
            public IEnumerable<Enhet> Underenheter { get; set; }
        }

        public class GeografiskAdresse
        {
            public string Adresse { get; set; }
            public string Postnummer { get; set; }
            public string PostSted { get; set; }
        }

        public class KodeListe{
            public string Kode { get; set; }
            public string Beskrivelse { get; set; }
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
                    Organisasjonsform =
                        new KodeListe
                        {
                            Kode = enhet.orgform_kode,
                            Beskrivelse = enhet.orgform_beskrivelse
                        },
                    Naeringskoder =
                        new[] {
                            new KodeListe { Kode = enhet.naeringskode1_kode, Beskrivelse = enhet.naeringskode1_beskrivelse },
                            new KodeListe { Kode = enhet.naeringskode2_kode, Beskrivelse = enhet.naeringskode2_beskrivelse },
                            new KodeListe { Kode = enhet.naeringskode3_kode, Beskrivelse = enhet.naeringskode3_beskrivelse }
                        }.Where(n => !String.IsNullOrEmpty(n.Kode)),
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
                    Organisasjonsform = null,
                    Naeringskoder = null,
                    Postadresse = null,
                    Forretningsadresse = null,
                    Underenheter = new Enhet[] {
                        new Enhet {
                            Organisasjonsnummer = underenhet.organisasjonsnummer,
                            Navn = underenhet.navn,
                            Organisasjonsform =
                                new KodeListe
                                {
                                    Kode = underenhet.orgform_kode,
                                    Beskrivelse = underenhet.orgform_beskrivelse
                                },
                            Naeringskoder =
                                new[] {
                                    new KodeListe { Kode = underenhet.naeringskode1_kode, Beskrivelse = underenhet.naeringskode1_beskrivelse },
                                    new KodeListe { Kode = underenhet.naeringskode2_kode, Beskrivelse = underenhet.naeringskode2_beskrivelse },
                                    new KodeListe { Kode = underenhet.naeringskode3_kode, Beskrivelse = underenhet.naeringskode3_beskrivelse }
                                }.Where(n => !String.IsNullOrEmpty(n.Kode)),
                            Postadresse = (String.IsNullOrEmpty(underenhet.postadresse_postnummer)) ? null :
                                new GeografiskAdresse
                                {
                                    Adresse = underenhet.postadresse_adresse,
                                    Postnummer = underenhet.postadresse_postnummer,
                                    PostSted = underenhet.postadresse_poststed
                                },
                            Beliggenhetsadresse = (String.IsNullOrEmpty(underenhet.beliggenhetsadresse_postnummer)) ? null :
                                new GeografiskAdresse
                                {
                                    Adresse = underenhet.beliggenhetsadresse_adresse,
                                    Postnummer = underenhet.beliggenhetsadresse_postnummer,
                                    PostSted = underenhet.beliggenhetsadresse_poststed
                                }                        
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
                    Organisasjonsform = g.Select(enhet => enhet.Organisasjonsform).FirstOrDefault(organisasjonsform => organisasjonsform != null),
                    Naeringskoder = g.Select(enhet => enhet.Naeringskoder).FirstOrDefault(naeringskoder => naeringskoder != null),
                    Postadresse = g.Select(enhet => enhet.Postadresse).FirstOrDefault(adresse => adresse != null),
                    Forretningsadresse = g.Select(enhet => enhet.Forretningsadresse).FirstOrDefault(adresse => adresse != null),
                    Underenheter = g.SelectMany(enhet => enhet.Underenheter)
                };

            //OutputReduceToCollection = "Enhet";
        }
    }
}
