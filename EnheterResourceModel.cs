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
                    where Char.IsDigit(metadata.Value<string>("@id")[8])
                    select new Resource
                    {
                        ResourceId =  enhet["orgnr"],
                        Type = new[] { "Enhet" },
                        SubType = new string [] { },
                        Title = new[] { enhet["navn"] },
                        SubTitle = new string[] { },
                        Code =  new[] { enhet["orgnr"] },
                        Body = new string[] { },
                        Status = 
                            from status in new[] { "konkurs", "avvikling", "tvangsavvikling" }
                            where enhet[status] == "J"
                            select status,
                        Tags = new string[] { },
                        Properties = (
                                new[] {
                                    new Property {
                                        Name = "Organisasjonsform",
                                        Resources = new[] {
                                            new Resource { ResourceId = "Organisasjonsform/" + enhet["organisasjonsform"] }
                                        }
                                    }
                                }
                            ).Union(
                                new[] {
                                    new Property {
                                        Name = "Næringskode",
                                        Resources =
                                            from naeringskode in new[] { enhet["nkode1"], enhet["nkode2"], enhet["nkode3"] }
                                            where !String.IsNullOrEmpty(naeringskode)
                                            select new Resource { ResourceId = "Næringskode/" + naeringskode }
                                    }
                                }
                            ).Union(
                                new[] {
                                    new Property {
                                        Name = "Sektorkode",
                                        Resources =
                                            from sektorkode in new[] { enhet["sektorkode"] }
                                            where !String.IsNullOrEmpty(sektorkode)
                                            select new Resource { ResourceId = "Sektorkode/" + sektorkode }
                                    }
                                }.Where(p => p.Resources.Any())
                            ).Union(
                                from adresse in new[] {
                                    new { navn = "Postadresse", adr = enhet["postadresse"], pnr = enhet["ppostnr"], psted = enhet["ppoststed"], land = enhet["ppostland"], knr = enhet["ppostkommnr"], kmn = enhet["ppostkommnavn"] },
                                    new { navn = "Forretningsadresse", adr = enhet["forretningsadr"], pnr = enhet["forradrpostnr"], psted = enhet["forradrpoststed"], land = enhet["forradrland"], knr = enhet["forradrkommnr"], kmn = enhet["forradrkommnavn"] }
                                }
                                where adresse.land != null
                                select new Property {
                                    Name = adresse.navn,
                                    Value = new[] {
                                        adresse.adr,
                                        ((adresse.pnr + " ") ?? "") + adresse.psted,
                                        enhet["ppostland"]
                                    }.Where(a => !String.IsNullOrEmpty(a)),
                                    Resources = (
                                        new[] {
                                            new Resource { Type = new[] { "Poststed" }, Code = new[] { adresse.pnr }, Title = new[] { adresse.psted } },
                                            new Resource { Type = new[] { "Kommune" }, Code = new[] { adresse.knr }, Title = new[] { adresse.kmn } }
                                        }
                                    ).Union(
                                        new[] {
                                            new Resource { Type = new[] { "Land" }, Title = new[] { adresse.land } }
                                        }
                                    ).Where(r => r.Title.Any(title => !String.IsNullOrEmpty(title)))
                                }
                            ).Union(
                                from overordnet in new[] { enhet["hovedenhet"] }
                                where !String.IsNullOrEmpty(overordnet)
                                select new Property {
                                    Name = "Overordnet",
                                    Resources = new[] {
                                        new Resource { ResourceId = overordnet }
                                    }
                                }
                            ).Union(
                                from ansatte in new[] { enhet["ansatte_antall"] }
                                where !String.IsNullOrEmpty(ansatte)
                                select new Property {
                                    Name = "Ansatte",
                                    Value = new[] { ansatte }
                                }
                            ),
                        Source = new[] { metadata.Value<string>("@id") }
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
                        SubTitle = new string[] { },
                        Code =  new[] { frivillig["orgnr"] },
                        Body = new string[] { },
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
                        SubTitle = new string[] { },
                        Code =  new[] { stotte["esaId"] },
                        Body = new string[] { },
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
                        Type = new[] { "Næringskode" },
                        SubType = new string[] { },
                        Title = new[] { naeringskode["shortName"] },
                        SubTitle = (naeringskode["shortName"] != naeringskode["name"]) ? new[] { naeringskode["name"] } : new string[] { },
                        Code =  new[] { naeringskode["code"] },
                        Body = new[] { naeringskode["notes"] },
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
                    from organisasjonsform in enheter
                    let metadata = MetadataFor(organisasjonsform)
                    where metadata.Value<string>("@id").StartsWith("Enheter/Organisasjonsform")
                    select new Resource
                    {
                        ResourceId = "Organisasjonsform/" + organisasjonsform["code"],
                        Type = new[] { "Organisasjonsform" },
                        SubType = new string[] { },
                        Title = new[] { organisasjonsform["name"] },
                        SubTitle = new string[] { },
                        Code =  new[] { organisasjonsform["code"] },
                        Body = new string[] { },
                        Status = new string[] { },
                        Tags = new string[] { },
                        Properties = new Property[] { },
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
                        Type = new[] { "Sektorkode" },
                        SubType = new string[] { },
                        Title = new[] { sektorkode["name"] },
                        SubTitle = new string[] { },
                        Code =  new[] { sektorkode["code"] },
                        Body = new[] { sektorkode["notes"] },
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

                Reduce = results =>
                    from result in results
                    group result by result.ResourceId into g
                    select new Resource
                    {
                        ResourceId = g.Key,
                        Type = g.SelectMany(r => r.Type).Distinct(),
                        SubType = g.SelectMany(r => r.SubType).Distinct(),
                        Title = g.SelectMany(r => r.Title).Distinct(),
                        SubTitle = g.SelectMany(r => r.SubTitle).Distinct(),
                        Code = g.SelectMany(r => r.Code).Distinct(),
                        Body = g.SelectMany(r => r.Body).Distinct(),
                        Status = g.SelectMany(r => r.Status).Distinct(),
                        Tags = g.SelectMany(r => r.Tags).Distinct(),
                        Properties = (IEnumerable<Property>)Properties(g.SelectMany(r => r.Properties)),
                        Source = g.SelectMany(resource => resource.Source).Distinct()
                    };

                Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

                OutputReduceToCollection = "EnheterResource";

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "ResourceModelUtils",
                        ReadResourceFile("enhetsregisteret_etl.ResourceModelUtils.cs")
                    }
                };
            }
        }
    }
}
