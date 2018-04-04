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
        public class Frivillighetsregisteret { }
        public class Stotteregisteret { }

        public class Enhet
        {
            public string Organisasjonsnummer { get; set; }
            public string Navn { get; set; }
            public KodeListe Organisasjonsform { get; set; }
            public KodeListe Sektorkode { get; set; }
            public IEnumerable<KodeListe> Naeringskoder { get; set; }
            public IEnumerable<KodeListe> Statuser { get; set; }
            public GeografiskAdresse Postadresse { get; set; }
            public GeografiskAdresse Forretningsadresse { get; set; }
            public GeografiskAdresse Beliggenhetsadresse { get; set; }
            public IEnumerable<Enhet> Overenheter { get; set; }
            public IEnumerable<Enhet> Underenheter { get; set; }
            public Frivillig Frivillig { get; set; }
            public IEnumerable<Stotte> Stotte { get; set; }
        }

        public class GeografiskAdresse
        {
            public string Adresse { get; set; }
            public string Postnummer { get; set; }
            public string PostSted { get; set; }
            public KodeListe Kommune { get; set; }
            public KodeListe Land { get; set; }
        }

        public class KodeListe
        {
            public string Kode { get; set; }
            public string Beskrivelse { get; set; }
        }

        public class Stotte
        {
            public DateTime Tildelingsdato { get; set; }
            public IEnumerable<decimal> Belop { get; set; }
            public string Valuta { get; set; }
            public string Navn { get; set; }
            public string Formaal { get; set; }
            public string Instrument { get; set; }
            public Enhet Spesifisert { get; set; }
            public Enhet Giver { get; set; }
        }

        public class Frivillig
        {
            public IEnumerable<KodeListe> Kategorier { get; set; }
        }

        public EnhetsregisteretIndex()
        {
            AddMap<Enhetsregisteret>(enheter =>
                from e in enheter
                let enhet = (IDictionary<string, string>)(object)e
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
                    Sektorkode =
                        new [] {
                            new KodeListe { Kode = enhet["institusjonellSektorkode.kode"], Beskrivelse = enhet["institusjonellSektorkode.beskrivelse"] }
                        }.FirstOrDefault(s => !String.IsNullOrEmpty(s.Kode)),
                    Naeringskoder =
                        new[] {
                            new KodeListe { Kode = enhet["naeringskode1.kode"], Beskrivelse = enhet["naeringskode1.beskrivelse"] },
                            new KodeListe { Kode = enhet["naeringskode2.kode"], Beskrivelse = enhet["naeringskode2.beskrivelse"] },
                            new KodeListe { Kode = enhet["naeringskode3.kode"], Beskrivelse = enhet["naeringskode3.beskrivelse"] }
                        }.Where(n => !String.IsNullOrEmpty(n.Kode)),
                    Statuser = 
                        from status in new[] { "konkurs", "underAvvikling", "underTvangsavviklingEllerTvangsopplosning" }
                        where enhet[status] == "J"
                        select new KodeListe { Kode = status },
                    Postadresse = 
                        new[] {
                            new GeografiskAdresse
                            {
                                Adresse = enhet["postadresse.adresse"],
                                Postnummer = enhet["postadresse.postnummer"],
                                PostSted = enhet["postadresse.poststed"],
                                Kommune = new KodeListe { Kode = enhet["postadresse.kommunenummer"], Beskrivelse = enhet["postadresse.kommune"] },
                                Land = new KodeListe { Kode = enhet["postadresse.landkode"], Beskrivelse = enhet["postadresse.land"] }
                            }
                        }.FirstOrDefault(a => !String.IsNullOrEmpty(a.Land.Kode)),
                    Forretningsadresse =
                        new[] {
                            new GeografiskAdresse
                            {
                                Adresse = enhet["forretningsadresse.adresse"],
                                Postnummer = enhet["forretningsadresse.postnummer"],
                                PostSted = enhet["forretningsadresse.poststed"],
                                Kommune = new KodeListe { Kode = enhet["forretningsadresse.kommunenummer"], Beskrivelse = enhet["forretningsadresse.kommune"] },
                                Land = new KodeListe { Kode = enhet["forretningsadresse.landkode"], Beskrivelse = enhet["forretningsadresse.land"] }
                            }
                        }.FirstOrDefault(a => !String.IsNullOrEmpty(a.Land.Kode)),
                    Beliggenhetsadresse =
                        new[] {
                            new GeografiskAdresse
                            {
                                Adresse = enhet["beliggenhetsadresse.adresse"],
                                Postnummer = enhet["beliggenhetsadresse.postnummer"],
                                PostSted = enhet["beliggenhetsadresse.poststed"],
                                Kommune = new KodeListe { Kode = enhet["beliggenhetsadresse.kommunenummer"], Beskrivelse = enhet["beliggenhetsadresse.kommune"] },
                                Land = new KodeListe { Kode = enhet["beliggenhetsadresse.landkode"], Beskrivelse = enhet["beliggenhetsadresse.land"] }
                            }
                        }.FirstOrDefault(a => !String.IsNullOrEmpty(a.Land.Kode)),
                    Overenheter = new Enhet[] { },
                    Underenheter = new Enhet[] { },
                    Frivillig = null,
                    Stotte = new Stotte[] { }
                }
            );

            AddMap<Enhetsregisteret>(enheter =>
                from e in enheter
                let enhet = (IDictionary<string, string>)(object)e
                where enhet.ContainsKey("overordnetEnhet")
                select new Enhet
                {
                    Organisasjonsnummer = enhet["organisasjonsnummer"],
                    Navn = null,
                    Organisasjonsform = null,
                    Sektorkode = null,
                    Naeringskoder = null,
                    Statuser = new KodeListe[] { },
                    Postadresse = null,
                    Forretningsadresse = null,
                    Beliggenhetsadresse = null,
                    Overenheter = (
                        from over in
                            Recurse(enhet,
                                o => (o.ContainsKey("overordnetEnhet")) ? LoadDocument<Enhetsregisteret>("Enhetsregisteret/" + o["overordnetEnhet"]) : null
                            ).Skip(1)
                        let overenhet = (IDictionary<string, string>)(object)over
                        select new Enhet
                        {
                            Organisasjonsnummer = overenhet["organisasjonsnummer"],
                            Navn = overenhet["navn"]
                        }).Reverse(),
                    Underenheter = new Enhet[] { },
                    Frivillig = null,
                    Stotte = new Stotte[] { }
                }
            );

            AddMap<Enhetsregisteret>(enheter =>
                from ue in enheter
                let underenhet = (IDictionary<string, string>)(object)ue
                where !String.IsNullOrEmpty(underenhet["overordnetEnhet"])
                select new Enhet
                {
                    Organisasjonsnummer = underenhet["overordnetEnhet"],
                    Navn = null,
                    Organisasjonsform = null,
                    Sektorkode = null,
                    Naeringskoder = null,
                    Statuser = new KodeListe[] { },
                    Postadresse = null,
                    Forretningsadresse = null,
                    Beliggenhetsadresse = null,
                    Overenheter = new Enhet[] { },
                    Underenheter = new Enhet[] {
                        new Enhet
                        {
                            Organisasjonsnummer = underenhet["organisasjonsnummer"],
                            Navn = underenhet["navn"]
                        }
                    },
                    Frivillig = null,
                    Stotte = new Stotte[] { }
                }
            );

            AddMap<Frivillighetsregisteret>(frivilligreg =>
                from f in frivilligreg
                let frivillig = (IDictionary<string, string>)(object)f
                select new Enhet
                {
                    Organisasjonsnummer = frivillig["orgnr"],
                    Navn = null,
                    Organisasjonsform = null,
                    Sektorkode = null,
                    Naeringskoder = null,
                    Statuser = new KodeListe[] { },
                    Postadresse = null,
                    Forretningsadresse = null,
                    Beliggenhetsadresse = null,
                    Overenheter = new Enhet[] { },
                    Underenheter = new Enhet[] { },
                    Frivillig =
                        new Frivillig
                        {
                            Kategorier =
                                new[] {
                                    new KodeListe { Kode = frivillig["kategori1"], Beskrivelse = frivillig["kategori1_tekst"] },
                                    new KodeListe { Kode = frivillig["kategori2"], Beskrivelse = frivillig["kategori2_tekst"] },
                                    new KodeListe { Kode = frivillig["kategori3"], Beskrivelse = frivillig["kategori3_tekst"] }
                                }.Where(n => !String.IsNullOrEmpty(n.Kode)),
                        },
                    Stotte = new Stotte[] { }
                }
            );

            AddMap<Stotteregisteret>(stottereg =>
                from s in stottereg
                let stotte = (IDictionary<string, string>)(object)s
                select new Enhet
                {
                    Organisasjonsnummer = stotte["stottemottakerOrganisasjonsnummer"],
                    Navn = null,
                    Organisasjonsform = null,
                    Sektorkode = null,
                    Naeringskoder = null,
                    Statuser = new KodeListe[] { },
                    Postadresse = null,
                    Forretningsadresse = null,
                    Beliggenhetsadresse = null,
                    Overenheter = new Enhet[] { },
                    Underenheter = new Enhet[] { },
                    Frivillig = null,
                    Stotte = new[] {
                        new Stotte {
                            Tildelingsdato = DateTime.ParseExact(stotte["tildelingsdato"], "dd.MM.yyyy", null),
                            Belop =
                                new[] {
                                    Decimal.Parse(stotte["belopFra"] ?? stotte["tildeltBelop"]),
                                    Decimal.Parse(stotte["belopTil"] ?? stotte["tildeltBelop"]),
                                }.Distinct(),
                            Valuta = stotte["valuta"],
                            Navn = stotte["navnStotteordning"],
                            Formaal = stotte["formaal"],
                            Instrument = stotte["stotteinstrument"],
                            Spesifisert =
                                new[] {
                                    new Enhet {
                                        Organisasjonsnummer = stotte["spesifisertStottemottakerOrganisasjonsnummer"],
                                        Navn = stotte["spesifisertStottemottakerNavn"] ?? stotte["spesifisertStottemottakerUtenOrganisasjonsnummer"]
                                    },
                                }.FirstOrDefault(s => !String.IsNullOrEmpty(s.Navn)),
                            Giver =
                                new Enhet
                                {
                                    Organisasjonsnummer = stotte["stottegiverOrganisasjonsnummer"],
                                    Navn = stotte["stottegiverNavn"]
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
                    Statuser = g.SelectMany(enhet => enhet.Statuser),
                    Postadresse = g.Select(enhet => enhet.Postadresse).FirstOrDefault(adresse => adresse != null),
                    Forretningsadresse = g.Select(enhet => enhet.Forretningsadresse).FirstOrDefault(adresse => adresse != null),
                    Beliggenhetsadresse = g.Select(enhet => enhet.Beliggenhetsadresse).FirstOrDefault(adresse => adresse != null),
                    Overenheter = g.SelectMany(enhet => enhet.Overenheter),
                    Underenheter = g.SelectMany(enhet => enhet.Underenheter),
                    Frivillig = g.Select(enhet => enhet.Frivillig).FirstOrDefault(frivillig => frivillig != null),
                    Stotte = g.SelectMany(enhet => enhet.Stotte)
                };

            //OutputReduceToCollection = "Enhet";
        }
    }
}
