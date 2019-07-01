using System;
using System.Collections.Generic;
using System.Linq;

namespace enhetsregisteret_etl
{
    public class ResourceModelUtils
    {

        public static IEnumerable<dynamic> Properties(IEnumerable<dynamic> properties)
        {
            foreach(var propertyG in ((IEnumerable<dynamic>)properties).GroupBy(p => p.Name))
            {
                if (propertyG.Any(p => p.Tags.Contains("@union")))
                {
                    yield
                        return new {
                            Name = propertyG.Key,
                            Tags = propertyG.SelectMany(p => (IEnumerable<dynamic>)p.Tags).Distinct(),
                            Resources = propertyG.SelectMany(p => (IEnumerable<dynamic>)p.Resources).Distinct(),
                        };
                }
                else
                {
                    yield return propertyG.First();
                }
            }
        }

        public static string ReadResourceFile(string filename)
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
