using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;

namespace enhetsregisteret_etl
{
    public class EnheterResourceModel
    {
        public class Enheter : Dictionary<string, string> { }

        public class Resource
        {
            public Resource() { }
            public string ResourceId { get; set; }
            public IEnumerable<string> Type { get; set; }
            public IEnumerable<string> SubType { get; set; }
            public IEnumerable<string> Title { get; set; }
            public IEnumerable<string> SubTitle { get; set; }
            public IEnumerable<string> Code { get; set; }
            public IEnumerable<string> Status { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Property> Properties { get; set; }
        }

        public class Property
        {
            public string Name { get; set; }
            public IEnumerable<string> Value { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Resource> Resources { get; set; }
            public IEnumerable<Property> Properties { get; set; }
        }

        public class EnheterResourceIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public EnheterResourceIndex()
            {
                AddMap<Enheter>(enheter =>
                    from enhet in enheter
                    let metadata = MetadataFor(enhet)
                    where metadata.Value<string>("@id").StartsWith("Enhetsregisteret")
                    select new Resource
                    {
                        ResourceId =  enhet["organisasjonsnummer"],
                        Type = new[] { "Enhet" },
                        SubType = new [] { enhet["orgform.beskrivelse"] },
                        Title = new[] { enhet["navn"] },
                        Code =  new[] { enhet["organisasjonsnummer"] },
                        Status = 
                            from status in new[] { "konkurs", "underAvvikling", "underTvangsavviklingEllerTvangsopplosning" }
                            where enhet[status] == "J"
                            select status,
                        Tags =
                            new[] {
                                enhet["naeringskode1.beskrivelse"],
                                enhet["naeringskode2.beskrivelse"],
                                enhet["naeringskode3.beskrivelse"],
                                enhet["institusjonellSektorkode.beskrivelse"]
                            }.Where(s => !String.IsNullOrEmpty(s)),
                        Properties = (
                                new[] {
                                    new Property {
                                        Name = "Organisasjonsform",
                                        Resources = new[] {
                                            new Resource { Code = new[] { enhet["orgform.kode"] }, Title = new[] { enhet["orgform.beskrivelse"] }}
                                        }
                                    }
                                }
                            ).Union(
                                new[] {
                                    new Property {
                                        Name = "Naeringskode",
                                        Resources = new[] {
                                            new Resource { Type = new[] { "Næringskode" }, Code = new[] { enhet["naeringskode1.kode"] }, Title = new[] { enhet["naeringskode1.beskrivelse"] } },
                                            new Resource { Type = new[] { "Næringskode" }, Code = new[] { enhet["naeringskode2.kode"] }, Title = new[] { enhet["naeringskode2.beskrivelse"] } },
                                            new Resource { Type = new[] { "Næringskode" }, Code = new[] { enhet["naeringskode3.kode"] }, Title = new[] { enhet["naeringskode3.beskrivelse"] } },
                                        }.Where(r => r.Code.Any(code => !String.IsNullOrEmpty(code)))
                                    }
                                }
                            ).Union(
                                new[] {
                                    new Property {
                                        Name = "Sektorkode",
                                        Resources = new[] {
                                            new Resource { Type = new[] { "Sektorkode" }, Code = new[] { enhet["institusjonellSektorkode.kode"] }, Title = new[] { enhet["institusjonellSektorkode.beskrivelse"] } }
                                        }.Where(r => r.Code.Any(code => !String.IsNullOrEmpty(code)))
                                    }
                                }
                            ).Union(
                                new[] {
                                    new Property {
                                        Name = "Postadresse",
                                        Value = new[] {
                                            enhet["postadresse.adresse"],
                                            enhet["postadresse.postnummer"] + " " + enhet["postadresse.poststed"] },
                                        Resources = new[] {
                                            new Resource { Type = new[] { "Poststed" }, Code = new[] { enhet["postadresse.postnummer"] }, Title = new[] { enhet["postadresse.poststed"] } },
                                            new Resource { Type = new[] { "Kommune" }, Code = new[] { enhet["postadresse.kommunenummer"] }, Title = new[] { enhet["postadresse.kommune"] } },
                                            new Resource { Type = new[] { "Land" }, Code = new[] { enhet["postadresse.landkode"] }, Title = new[] { enhet["postadresse.land"] } }
                                        }
                                    },
                                    new Property {
                                        Name = "Forretningsadresse",
                                        Value = new[] {
                                            enhet["forretningsadresse.adresse"],
                                            enhet["forretningsadresse.postnummer"] + " " + enhet["forretningsadresse.poststed"] },
                                        Resources = new[] {
                                            new Resource { Type = new[] { "Poststed" }, Code = new[] { enhet["forretningsadresse.postnummer"] }, Title = new[] { enhet["forretningsadresse.poststed"] } },
                                            new Resource { Type = new[] { "Kommune" }, Code = new[] { enhet["forretningsadresse.kommunenummer"] }, Title = new[] { enhet["forretningsadresse.kommune"] } },
                                            new Resource { Type = new[] { "Land" }, Code = new[] { enhet["forretningsadresse.landkode"] }, Title = new[] { enhet["forretningsadresse.land"] } }
                                        }
                                    },
                                    new Property {
                                        Name = "Beliggenhetsadresse",
                                        Value = new[] {
                                            enhet["beliggenhetsadresse.adresse"],
                                            enhet["beliggenhetsadresse.postnummer"] + " " + enhet["beliggenhetsadresse.poststed"] },
                                        Resources = new[] {
                                            new Resource { Type = new[] { "Poststed" }, Code = new[] { enhet["beliggenhetsadresse.postnummer"] }, Title = new[] { enhet["beliggenhetsadresse.poststed"] } },
                                            new Resource { Type = new[] { "Kommune" }, Code = new[] { enhet["beliggenhetsadresse.kommunenummer"] }, Title = new[] { enhet["beliggenhetsadresse.kommune"] } },
                                            new Resource { Type = new[] { "Land" }, Code = new[] { enhet["beliggenhetsadresse.landkode"] }, Title = new[] { enhet["beliggenhetsadresse.land"] } }
                                        }
                                    }
                                }.Where(p => p.Value.Any(v => !String.IsNullOrWhiteSpace(v)))
                            )
                    }
                );

                AddMap<Enheter>(enheter =>
                    from enhet in enheter
                    where MetadataFor(enhet).Value<string>("@id").StartsWith("Enhetsregisteret") && !String.IsNullOrEmpty(enhet["overordnetEnhet"])
                    select new Resource
                    {
                        ResourceId = enhet["organisasjonsnummer"],
                        Type = new string[] { },
                        SubType = new string[] { },
                        Title = new string[] { },
                        Code =  new string[] { },
                        Status = new string[] { },
                        Tags = new string[] { },
                        Properties = new[] {
                            new Property {
                                Name = "Overordnet",
                                Resources = new[] { new Resource { Type = new[] { "Enhet" }, Code = new[] { enhet["overordnetEnhet"] }} }
                            }
                        }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from frivillig in enheter
                    let metadata = MetadataFor(frivillig)
                    where metadata.Value<string>("@id").StartsWith("Frivillighetsregisteret")
                    select new Resource
                    {
                        ResourceId =  frivillig["orgnr"],
                        Type = new string[] { },
                        SubType = new string[] { },
                        Title = new string[] { },
                        Code =  new string[] { },
                        Status = new string[] { "frivillig" },
                        Tags =
                            new[] {
                                frivillig["kategori1_tekst"],
                                frivillig["kategori2_tekst"],
                                frivillig["kategori3_tekst"]
                            }.Where(s => !String.IsNullOrEmpty(s)),
                        Properties = new[] {
                            new Property {
                                Name = "Aktivitetskategori",
                                Resources = new[] {
                                    new Resource { Type = new[] { "Aktivitetskategori" }, Code = new[] { frivillig["kategori1"] }, Title = new[] { frivillig["kategori1_tekst"] } },
                                    new Resource { Type = new[] { "Aktivitetskategori" }, Code = new[] { frivillig["kategori2"] }, Title = new[] { frivillig["kategori2_tekst"] } },
                                    new Resource { Type = new[] { "Aktivitetskategori" }, Code = new[] { frivillig["kategori3"] }, Title = new[] { frivillig["kategori3_tekst"] } },
                                }.Where(r => r.Code.Any(code => !String.IsNullOrEmpty(code)))
                            }
                        }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from stotte in enheter
                    let metadata = MetadataFor(stotte)
                    where metadata.Value<string>("@id").StartsWith("Stotteregisteret")
                    select new Resource
                    {
                        ResourceId = stotte["stottemottakerOrganisasjonsnummer"],
                        Type = new string[] { },
                        SubType = new string[] { },
                        Title = new string[] { },
                        Code =  new string[] { },
                        Status = new string[] { "stottemottaker" },
                        Tags =
                            new[] {
                                stotte["navnStotteordning"],
                                stotte["formaal"],
                                stotte["stotteinstrument"]
                            }.Where(s => !String.IsNullOrEmpty(s)),
                        Properties = new Property[] { }
                    }
                );

                Reduce = results  =>
                    from result in results
                    group result by result.ResourceId into g
                    select new Resource
                    {
                        ResourceId = g.Key,
                        Type = g.SelectMany(resource => resource.Type).Distinct(),
                        SubType = g.SelectMany(resource => resource.SubType).Distinct(),
                        Title = g.SelectMany(resource => resource.Title).Distinct(),
                        Code = g.SelectMany(resource => resource.Code).Distinct(),
                        Status = g.SelectMany(resource => resource.Status).Distinct(),
                        Tags = g.SelectMany(resource => resource.Tags).Distinct(),
                        Properties = g.SelectMany(resource => resource.Properties)
                    };

                OutputReduceToCollection = "EnheterResource";
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                var indexDefinition = base.CreateIndexDefinition();

                return new IndexDefinition
                {
                    Name = indexDefinition.Name,
                    Maps = indexDefinition.Maps,
                    Reduce = indexDefinition.Reduce,
                    OutputReduceToCollection = indexDefinition.OutputReduceToCollection,
                    Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "30"} }
                };
            }
        }
    }
}
