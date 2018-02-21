using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;

namespace Enhetsregisteret
{
    public class EnhetsregisteretIndex : AbstractMultiMapIndexCreationTask<EnhetsregisteretIndex.Enhet>
    {
        public class Enhetsregisteret { }

        public class Enhet
        {
            public string Organisasjonsnummer { get; set; }
            public string Navn { get; set; }
            public KodeListe Organisasjonsform { get; set; }
            public KodeListe Sektorkode { get; set; }
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
            public KodeListe Kommune { get; set; }
            public KodeListe Land { get; set; }
        }

        public class KodeListe{
            public string Kode { get; set; }
            public string Beskrivelse { get; set; }
        }

        public EnhetsregisteretIndex()
        {
            AddMap<Enhetsregisteret>(enheter =>
                from e in enheter
                let enhet = (IDictionary<string, string>)(object)e
                where !(new[] { "BEDR", "AAFY"}.Contains(enhet["orgform.kode"]))
                select new Enhet
                {
                    Organisasjonsnummer = enhet["organisasjonsnummer"],
                    Navn = enhet["navn"],
                    Organisasjonsform =
                        new KodeListe
                        {
                            Kode = enhet["orgform.kode"],
                            Beskrivelse = enhet["orgform.beskrivelse"]
                        },
                   Sektorkode = (String.IsNullOrEmpty(enhet["institusjonellSektorkode.kode"])) ? null :
                        new KodeListe
                        {
                            Kode = enhet["institusjonellSektorkode.kode"],
                            Beskrivelse = enhet["institusjonellSektorkode.beskrivelse"]
                        },
                    Naeringskoder =
                        new[] {
                            new KodeListe { Kode = enhet["naeringskode1.kode"], Beskrivelse = enhet["naeringskode1.beskrivelse"] },
                            new KodeListe { Kode = enhet["naeringskode2.kode"], Beskrivelse = enhet["naeringskode2.beskrivelse"] },
                            new KodeListe { Kode = enhet["naeringskode3.kode"], Beskrivelse = enhet["naeringskode3.beskrivelse"] }
                        }.Where(n => !String.IsNullOrEmpty(n.Kode)),
                    Postadresse = (String.IsNullOrEmpty(enhet["postadresse.landkode"])) ? null :
                        new GeografiskAdresse
                        {
                            Adresse = enhet["postadresse.adresse"],
                            Postnummer = enhet["postadresse.postnummer"],
                            PostSted = enhet["postadresse.poststed"],
                            Kommune = new KodeListe { Kode = enhet["postadresse.kommunenummer"], Beskrivelse = enhet["postadresse.kommune"] },
                            Land = new KodeListe { Kode = enhet["postadresse.landkode"], Beskrivelse = enhet["postadresse.land"] }
                        },
                    Forretningsadresse = (String.IsNullOrEmpty(enhet["forretningsadresse.landkode"])) ? null :
                        new GeografiskAdresse
                        {
                            Adresse = enhet["forretningsadresse.adresse"],
                            Postnummer = enhet["forretningsadresse.postnummer"],
                            PostSted = enhet["forretningsadresse.poststed"],
                            Kommune = new KodeListe { Kode = enhet["forretningsadresse.kommunenummer"], Beskrivelse = enhet["forretningsadresse.kommune"] },
                            Land = new KodeListe { Kode = enhet["forretningsadresse.landkode"], Beskrivelse = enhet["forretningsadresse.land"] }
                        },                        
                    Underenheter = new Enhet[] { }
                }
            );

            AddMap<Enhetsregisteret>(enheter =>
                from ue in enheter
                let underenhet = (IDictionary<string, string>)(object)ue
                where new[] { "BEDR", "AAFY"}.Contains(underenhet["orgform.kode"])
                select new Enhet
                {
                    Organisasjonsnummer = underenhet["overordnetEnhet"],
                    Navn = null,
                    Organisasjonsform = null,
                    Sektorkode = null,
                    Naeringskoder = null,
                    Postadresse = null,
                    Forretningsadresse = null,
                    Underenheter = new Enhet[] {
                        new Enhet {
                            Organisasjonsnummer = underenhet["organisasjonsnummer"],
                            Navn = underenhet["navn"],
                            Organisasjonsform =
                                new KodeListe
                                {
                                    Kode = underenhet["orgform.kode"],
                                    Beskrivelse = underenhet["orgform.beskrivelse"]
                                },
                            Naeringskoder =
                                new[] {
                                    new KodeListe { Kode = underenhet["naeringskode1.kode"], Beskrivelse = underenhet["naeringskode1.beskrivelse"] },
                                    new KodeListe { Kode = underenhet["naeringskode2.kode"], Beskrivelse = underenhet["naeringskode2.beskrivelse"] },
                                    new KodeListe { Kode = underenhet["naeringskode3.kode"], Beskrivelse = underenhet["naeringskode3.beskrivelse"] }
                                }.Where(n => !String.IsNullOrEmpty(n.Kode)),
                            Postadresse = (String.IsNullOrEmpty(underenhet["postadresse.landkode"])) ? null :
                                new GeografiskAdresse
                                {
                                    Adresse = underenhet["postadresse.adresse"],
                                    Postnummer = underenhet["postadresse.postnummer"],
                                    PostSted = underenhet["postadresse.poststed"],
                                    Kommune = new KodeListe { Kode = underenhet["postadresse.kommunenummer"], Beskrivelse = underenhet["postadresse.kommune"] },
                                    Land = new KodeListe { Kode = underenhet["postadresse.landkode"], Beskrivelse = underenhet["postadresse.land"] }                                    
                                },
                            Beliggenhetsadresse = (String.IsNullOrEmpty(underenhet["beliggenhetsadresse.landkode"])) ? null :
                                new GeografiskAdresse
                                {
                                    Adresse = underenhet["beliggenhetsadresse.adresse"],
                                    Postnummer = underenhet["beliggenhetsadresse.postnummer"],
                                    PostSted = underenhet["beliggenhetsadresse.poststed"],
                                    Kommune = new KodeListe { Kode = underenhet["beliggenhetsadresse.kommunenummer"], Beskrivelse = underenhet["beliggenhetsadresse.kommune"] },
                                    Land = new KodeListe { Kode = underenhet["beliggenhetsadresse.landkode"], Beskrivelse = underenhet["beliggenhetsadresse.land"] }
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
                    Sektorkode = g.Select(enhet => enhet.Sektorkode).FirstOrDefault(sektorkode => sektorkode != null),
                    Naeringskoder = g.Select(enhet => enhet.Naeringskoder).FirstOrDefault(naeringskoder => naeringskoder != null),
                    Postadresse = g.Select(enhet => enhet.Postadresse).FirstOrDefault(adresse => adresse != null),
                    Forretningsadresse = g.Select(enhet => enhet.Forretningsadresse).FirstOrDefault(adresse => adresse != null),
                    Underenheter = g.SelectMany(enhet => enhet.Underenheter)
                };

            //OutputReduceToCollection = "Enhet";
        }
    }
}
