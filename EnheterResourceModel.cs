using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static enhetsregisteret_etl.ResourceModel;
using static enhetsregisteret_etl.ResourceModelUtils;

namespace enhetsregisteret_etl
{
    public class EnheterResourceModel
    {
        public class Enheter : Dictionary<string, string> { }

        public class EnheterResourceIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public EnheterResourceIndex()
            {
                AddMap<Enheter>(enheter =>
                    from enhet in enheter
                    let metadata = MetadataFor(enhet)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Enhetsregisteret")
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
                                            new Resource { Code = new[] { enhet["naeringskode1.kode"] }, Title = new[] { enhet["naeringskode1.beskrivelse"] } },
                                            new Resource { Code = new[] { enhet["naeringskode2.kode"] }, Title = new[] { enhet["naeringskode2.beskrivelse"] } },
                                            new Resource { Code = new[] { enhet["naeringskode3.kode"] }, Title = new[] { enhet["naeringskode3.beskrivelse"] } },
                                        }.Where(r => r.Code.Any(code => !String.IsNullOrEmpty(code)))
                                    }
                                }
                            ).Union(
                                new[] {
                                    new Property {
                                        Name = "Sektorkode",
                                        Resources = new[] {
                                            new Resource { Code = new[] { enhet["institusjonellSektorkode.kode"] }, Title = new[] { enhet["institusjonellSektorkode.beskrivelse"] } }
                                        }.Where(r => r.Code.Any(code => !String.IsNullOrEmpty(code)))
                                    }
                                }.Where(p => p.Resources.Any())
                            ).Union(
                                from adressenavn in new[] { "Postadresse", "Forretningsadresse", "Beliggenhetsadresse" }
                                let adresse = adressenavn.ToLower()
                                where enhet[adresse + ".landkode"] != null
                                select new Property {
                                    Name = adressenavn,
                                    Value = new[] {
                                        enhet[adresse + ".adresse"],
                                        ((enhet[adresse + ".postnummer"] + " ") ?? "") + enhet[adresse + ".poststed"],
                                        (enhet[adresse + ".landkode"] != "NO") ? enhet[adresse + ".land"] : ""
                                    }.Where(a => !String.IsNullOrEmpty(a)),
                                    Resources = new[] {
                                        new Resource { Type = new[] { "Poststed" }, Code = new[] { enhet[adresse + ".postnummer"] }, Title = new[] { enhet[adresse + ".poststed"] } },
                                        new Resource { Type = new[] { "Kommune" }, Code = new[] { enhet[adresse + ".kommunenummer"] }, Title = new[] { enhet[adresse + ".kommune"] } },
                                        new Resource { Type = new[] { "Land" }, Code = new[] { enhet[adresse + ".landkode"] }, Title = new[] { enhet[adresse + ".land"] } }
                                    }.Where(r => r.Code.Any(code => !String.IsNullOrEmpty(code)))
                                }
                            ).Union(
                                from overordnet in new[] { enhet["overordnetEnhet"] }
                                where !String.IsNullOrEmpty(overordnet)
                                select new Property {
                                    Name = "Overordnet",
                                    Resources = new[] {
                                        new Resource { ResourceId = overordnet }
                                    }
                                }
                            ),
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from enhet in enheter
                    let metadata = MetadataFor(enhet)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Enhetsregisteret") && !String.IsNullOrEmpty(enhet["overordnetEnhet"])

                    select new Resource
                    {
                        ResourceId = enhet["overordnetEnhet"],
                        Type = new string[] { },
                        SubType = new string[] { },
                        Title = new string[] { },
                        Code =  new string[] { },
                        Status = new string[] { },
                        Tags = new string[] { },
                        Properties = new[] {
                            new Property {
                                Name = "Underordnet",
                                Tags = new[] { "@union" },
                                Resources = new[] {
                                    new Resource { ResourceId = enhet["organisasjonsnummer"] }
                                },
                                Source = new[] { metadata.Value<string>("@id") }
                            }
                        },
                        Source = new string[] { }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from frivillig in enheter
                    let metadata = MetadataFor(frivillig)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Frivillighetsregisteret")
                    select new Resource
                    {
                        ResourceId = frivillig["orgnr"],
                        Type = new[] { "Frivillig" },
                        SubType = new string[] { },
                        Title = new[] { frivillig["navn"] },
                        Code =  new[] { frivillig["orgnr"] },
                        Status = 
                            from status in new[] { "vedtekter", "arsregnskap", "grasrotandel" }
                            where frivillig[status] == "J"
                            select status,
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
                        },
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from stotte in enheter
                    let metadata = MetadataFor(stotte)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Stotteregisteret")
                    select new Resource
                    {
                        ResourceId = stotte["tildelingId"],
                        Type = new[] { stotte["typeTiltak"] },
                        SubType = new[] { stotte["stotteinstrument"] },
                        Title = new[] { stotte["navnStotteordning"], stotte["navnStottetiltak"] }.Where(t => t != null).Select(t => t.Trim()).Distinct(),
                        Code =  new[] { stotte["esaId"] },
                        Status = new string[] { },
                        Tags = new[] { stotte["formaal"] },
                        Properties = new[] {
                            new Property { Name = "Mottaker" , Resources = new[] { new Resource { ResourceId = stotte["stottemottakerOrganisasjonsnummer"] } } },
                            new Property { Name = "Giver" , Resources = new[] { new Resource { ResourceId = stotte["stottegiverOrganisasjonsnummer"] } } }
                        },
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from naeringskode in enheter
                    let metadata = MetadataFor(naeringskode)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Naeringskode")
                    select new Resource
                    {
                        ResourceId = "Næringskode/" + naeringskode["code"],
                        Type = new string[] { "Næringskode" },
                        SubType = new string[] { },
                        Title = new string[] { naeringskode["name"] },
                        Code =  new string[] { naeringskode["code"] },
                        Status = new string[] { },
                        Tags = new string[] { },
                        Properties = (
                            new[] {
                                new Property {
                                    Name = "Klassifisering",
                                    Resources = 
                                        from o in Recurse(naeringskode, n => LoadDocument<Enheter>("Enheter/Naeringskode/" + n["parentCode"])).Skip(1).Reverse()
                                        where o != null
                                        select new Resource
                                        {
                                            ResourceId = "Næringskode/" + o["code"]
                                        }
                                }
                            }.Where(p => p.Resources.Any())
                        ),
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from sektorkode in enheter
                    let metadata = MetadataFor(sektorkode)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Sektorkode")
                    select new Resource
                    {
                        ResourceId = "Sektorkode/" + sektorkode["code"],
                        Type = new string[] { "Sektorkode" },
                        SubType = new string[] { },
                        Title = new string[] { sektorkode["name"] },
                        Code =  new string[] { sektorkode["code"] },
                        Status = new string[] { },
                        Tags = new string[] { },
                        Properties = (
                            new[] {
                                new Property {
                                    Name = "Klassifisering",
                                    Resources = 
                                        from o in Recurse(sektorkode, n => LoadDocument<Enheter>("Enheter/Sektorkode/" + n["parentCode"])).Skip(1).Reverse()
                                        where o != null
                                        select new Resource
                                        {
                                            ResourceId = "Sektorkode/" + o["code"]
                                        }
                                }
                            }.Where(p => p.Resources.Any())
                        ),
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                Reduce = results  =>
                    from result in results
                    group result by result.ResourceId into g
                    select new Resource
                    {
                        ResourceId = g.Key,
                        Type = g.SelectMany(r => r.Type).Distinct(),
                        SubType = g.SelectMany(r => r.SubType).Distinct(),
                        Title = g.SelectMany(r => r.Title).Distinct(),
                        Code = g.SelectMany(r => r.Code).Distinct(),
                        Status = g.SelectMany(r => r.Status).Distinct(),
                        Tags = g.SelectMany(r => r.Tags).Distinct(),
                        Properties = (IEnumerable<Property>)Properties(g.SelectMany(r => r.Properties)),
                        Source = g.SelectMany(resource => resource.Source).Distinct()
                    };

                Index(r => r.Properties, FieldIndexing.No);
                Store(r => r.Properties, FieldStorage.Yes);

                OutputReduceToCollection = "EnheterResource";

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "ResourceModel",
                        ReadResourceFile("enhetsregisteret_etl.ResourceModelUtils.cs")
                    }
                };
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                var indexDefinition = base.CreateIndexDefinition();
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "8192"} };

                return indexDefinition;
            }
        }
    }
}
