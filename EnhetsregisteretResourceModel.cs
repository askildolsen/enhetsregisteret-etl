using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;

namespace enhetsregisteret_etl
{
    public class EnhetsRegisteretResourceModel
    {
        public class Enhetsregisteret : Dictionary<string, string> { }
        public class Frivillighetsregisteret : Dictionary<string, string> { }
        public class Stotteregisteret : Dictionary<string, string> { }

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

        public class EnhetsregisteretResourceIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public EnhetsregisteretResourceIndex()
            {
                AddMap<Enhetsregisteret>(enheter =>
                    from enhet in enheter
                    select new Resource
                    {
                        ResourceId =  enhet["organisasjonsnummer"],
                        Type = new[] { "Enhet", enhet["orgform.kode"] + "|" + enhet["orgform.beskrivelse"] },
                        SubType = new [] { enhet["institusjonellSektorkode.kode"] + "|" + enhet["institusjonellSektorkode.beskrivelse"] }.Where(s => s != "|"),
                        Title = new[] { enhet["navn"] },
                        Code =  new[] { enhet["organisasjonsnummer"] },
                        Status = 
                            from status in new[] { "konkurs", "underAvvikling", "underTvangsavviklingEllerTvangsopplosning" }
                            where enhet[status] == "J"
                            select status,
                        Tags =
                            new[] {
                                enhet["naeringskode1.kode"] + "|" + enhet["naeringskode1.beskrivelse"],
                                enhet["naeringskode2.kode"] + "|" + enhet["naeringskode2.beskrivelse"],
                                enhet["naeringskode3.kode"] + "|" + enhet["naeringskode3.beskrivelse"]
                            }.Where(s => s != "|"),
                        Properties = new[] {
                            new Property
                            {
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
                            new Property
                            {
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
                            new Property
                            {
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
                    }
                );

                AddMap<Frivillighetsregisteret>(frivilligreg =>
                    from frivillig in frivilligreg
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
                                frivillig["kategori1"] + "|" + frivillig["kategori1_tekst"],
                                frivillig["kategori2"] + "|" + frivillig["kategori2_tekst"],
                                frivillig["kategori3"] + "|" + frivillig["kategori3_tekst"]
                            }.Where(s => !String.IsNullOrEmpty(s)),
                        Properties = new Property[] { }
                    }
                );

                AddMap<Stotteregisteret>(stottereg =>
                    from stotte in stottereg
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

                OutputReduceToCollection = "EnhetsregisteretResource";
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
