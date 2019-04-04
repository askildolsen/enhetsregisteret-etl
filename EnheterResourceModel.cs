using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using static enhetsregisteret_etl.ResourceModel;

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
                                        new Property.Resource { Type = new[] { "Enhet" }, Code = new[] { overordnet }, Target = ResourceTarget("Enheter", overordnet) }
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
                                    new Property.Resource { Type = new[] { "Enhet" }, Code = new[] { enhet["organisasjonsnummer"] }, Target = ResourceTarget("Enheter", enhet["organisasjonsnummer"]) }
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
                        && LoadDocument<Enheter>("Enheter/Enhetsregisteret/" + frivillig["orgnr"]) != null
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
                        },
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from stotte in enheter
                    let metadata = MetadataFor(stotte)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Stotteregisteret")
                        && LoadDocument<Enheter>("Enheter/Enhetsregisteret/" + stotte["stottemottakerOrganisasjonsnummer"]) != null
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
                        Properties = new Property[] { },
                        Source = new[] { metadata.Value<string>("@id") }
                    }
                );

                AddMap<Enheter>(enheter =>
                    from enhet in enheter
                    let metadata = MetadataFor(enhet)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Enhetsregisteret")

                    let resources = (
                        from adresse in new[] { "postadresse", "forretningsadresse", "beliggenhetsadresse" }
                        where enhet[adresse + ".landkode"] != null

                        from resource in new[] {
                            new Resource { Type = new[] { "Poststed" }, Code = new[] { enhet[adresse + ".postnummer"] }, Title = new[] { enhet[adresse + ".poststed"] } },
                            new Resource { Type = new[] { "Kommune" }, Code = new[] { enhet[adresse + ".kommunenummer"] }, Title = new[] { enhet[adresse + ".kommune"] } },
                            new Resource { Type = new[] { "Land" }, Code = new[] { enhet[adresse + ".landkode"] }, Title = new[] { enhet[adresse + ".land"] } }
                        }

                        where resource.Code.Any(code => !String.IsNullOrEmpty(code))
                        select resource
                    )

                    from resource in resources
                    group resource by new { resource.Type, resource.Code } into g

                    select new Resource
                    {
                        ResourceId = g.First().Type.First() + "/" + g.First().Code.First(),
                        Type = g.First().Type,
                        SubType = new string[] { },
                        Title = g.First().Title,
                        Code =  g.First().Code,
                        Status = new string[] { },
                        Tags = new string[] { },
                        Properties = new Property[] { },
                        Source = new string[] { }
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
                                        select new Property.Resource
                                        {
                                            Target = ResourceTarget("Enheter", "Næringskode/" + o["code"])
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
                                        select new Property.Resource
                                        {
                                            Target = ResourceTarget("Enheter", "Sektorkode/" + o["code"])
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
                        Properties = (
                            g.SelectMany(r => r.Properties).Where(p => !p.Tags.Contains("@union"))
                        ).Union(
                            from property in g.SelectMany(r => r.Properties).Where(p => p.Tags.Contains("@union"))
                            group property by property.Name into propertyG
                            select
                                new Property {
                                    Name = propertyG.Key,
                                    Tags = propertyG.SelectMany(p => p.Tags).Distinct(),
                                    Resources = propertyG.SelectMany(p => p.Resources).Distinct(),
                                    Source = propertyG.SelectMany(p => p.Source).Distinct()
                                }
                        ),
                        Source = g.SelectMany(resource => resource.Source).Distinct()
                    };

                Index(r => r.Properties, FieldIndexing.No);
                Store(r => r.Properties, FieldStorage.Yes);

                OutputReduceToCollection = "EnheterResource";

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "ResourceModel",
                        ReadResourceFile("enhetsregisteret_etl.ResourceModel.cs")
                    }
                };
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                var indexDefinition = base.CreateIndexDefinition();

                return new IndexDefinition
                {
                    Name = indexDefinition.Name,
                    Maps = indexDefinition.Maps,
                    Reduce = indexDefinition.Reduce,
                    Fields = indexDefinition.Fields,
                    OutputReduceToCollection = indexDefinition.OutputReduceToCollection,
                    AdditionalSources = indexDefinition.AdditionalSources,
                    Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "120"} }
                };
            }
        }

        private static string ReadResourceFile(string filename)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var stream = assembly.GetManifestResourceStream(filename))
            {
                using (var reader = new System.IO.StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
