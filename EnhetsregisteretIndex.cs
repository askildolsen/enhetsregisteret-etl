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
            public string postadresse_kommunenummer { get; set; }
            public string postadresse_kommune { get; set; }
            public string postadresse_landkode { get; set; }
            public string postadresse_land { get; set; }
            public string forretningsadresse_adresse { get; set; }
            public string forretningsadresse_postnummer { get; set; }
            public string forretningsadresse_poststed { get; set; }
            public string forretningsadresse_kommunenummer { get; set; }
            public string forretningsadresse_kommune { get; set; }
            public string forretningsadresse_landkode { get; set; }
            public string forretningsadresse_land { get; set; }
            public string beliggenhetsadresse_adresse { get; set; }
            public string beliggenhetsadresse_postnummer { get; set; }
            public string beliggenhetsadresse_poststed { get; set; }
            public string beliggenhetsadresse_kommunenummer { get; set; }
            public string beliggenhetsadresse_kommune { get; set; }
            public string beliggenhetsadresse_landkode { get; set; }
            public string beliggenhetsadresse_land { get; set; }
            public string institusjonellSektorkode_kode { get; set; }
            public string institusjonellSektorkode_beskrivelse { get; set; }
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
                from enhet in enheter
                where !(new[] { "BEDR", "AAFY"}.Contains(enhet.orgform_kode))
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
                   Sektorkode = (String.IsNullOrEmpty(enhet.institusjonellSektorkode_kode)) ? null :
                        new KodeListe
                        {
                            Kode = enhet.institusjonellSektorkode_kode,
                            Beskrivelse = enhet.institusjonellSektorkode_beskrivelse
                        },
                    Naeringskoder =
                        new[] {
                            new KodeListe { Kode = enhet.naeringskode1_kode, Beskrivelse = enhet.naeringskode1_beskrivelse },
                            new KodeListe { Kode = enhet.naeringskode2_kode, Beskrivelse = enhet.naeringskode2_beskrivelse },
                            new KodeListe { Kode = enhet.naeringskode3_kode, Beskrivelse = enhet.naeringskode3_beskrivelse }
                        }.Where(n => !String.IsNullOrEmpty(n.Kode)),
                    Postadresse = (String.IsNullOrEmpty(enhet.postadresse_landkode)) ? null :
                        new GeografiskAdresse
                        {
                            Adresse = enhet.postadresse_adresse,
                            Postnummer = enhet.postadresse_postnummer,
                            PostSted = enhet.postadresse_poststed,
                            Kommune = new KodeListe { Kode = enhet.postadresse_kommunenummer, Beskrivelse = enhet.postadresse_kommune },
                            Land = new KodeListe { Kode = enhet.postadresse_landkode, Beskrivelse = enhet.postadresse_land }
                        },
                    Forretningsadresse = (String.IsNullOrEmpty(enhet.forretningsadresse_landkode)) ? null :
                        new GeografiskAdresse
                        {
                            Adresse = enhet.forretningsadresse_adresse,
                            Postnummer = enhet.forretningsadresse_postnummer,
                            PostSted = enhet.forretningsadresse_poststed,
                            Kommune = new KodeListe { Kode = enhet.forretningsadresse_kommunenummer, Beskrivelse = enhet.forretningsadresse_kommune },
                            Land = new KodeListe { Kode = enhet.forretningsadresse_landkode, Beskrivelse = enhet.forretningsadresse_land }
                        },                        
                    Underenheter = new Enhet[] { }
                }
            );

            AddMap<Enhetsregisteret>(enheter =>
                from underenhet in enheter
                where new[] { "BEDR", "AAFY"}.Contains(underenhet.orgform_kode)
                select new Enhet
                {
                    Organisasjonsnummer = underenhet.overordnetEnhet,
                    Navn = null,
                    Organisasjonsform = null,
                    Sektorkode = null,
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
                            Postadresse = (String.IsNullOrEmpty(underenhet.postadresse_landkode)) ? null :
                                new GeografiskAdresse
                                {
                                    Adresse = underenhet.postadresse_adresse,
                                    Postnummer = underenhet.postadresse_postnummer,
                                    PostSted = underenhet.postadresse_poststed,
                                    Kommune = new KodeListe { Kode = underenhet.postadresse_kommunenummer, Beskrivelse = underenhet.postadresse_kommune },
                                    Land = new KodeListe { Kode = underenhet.postadresse_landkode, Beskrivelse = underenhet.postadresse_land }                                    
                                },
                            Beliggenhetsadresse = (String.IsNullOrEmpty(underenhet.beliggenhetsadresse_landkode)) ? null :
                                new GeografiskAdresse
                                {
                                    Adresse = underenhet.beliggenhetsadresse_adresse,
                                    Postnummer = underenhet.beliggenhetsadresse_postnummer,
                                    PostSted = underenhet.beliggenhetsadresse_poststed,
                                    Kommune = new KodeListe { Kode = underenhet.beliggenhetsadresse_kommunenummer, Beskrivelse = underenhet.beliggenhetsadresse_kommune },
                                    Land = new KodeListe { Kode = underenhet.beliggenhetsadresse_landkode, Beskrivelse = underenhet.beliggenhetsadresse_land }
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
