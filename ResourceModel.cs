using System;
using System.Collections.Generic;

namespace enhetsregisteret_etl
{
    public class ResourceModel
    {
        public class Resource
        {
            public string ResourceId { get; set; }
            public IEnumerable<string> Type { get; set; }
            public IEnumerable<string> SubType { get; set; }
            public IEnumerable<string> Title { get; set; }
            public IEnumerable<string> SubTitle { get; set; }
            public IEnumerable<string> Code { get; set; }
            public IEnumerable<string> Status { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }
        }

        public class Property
        {
            public string Name { get; set; }
            public IEnumerable<string> Value { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<ResourceModel.Resource> Resources { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }

            public class Resource : ResourceModel.Resource {
                public string Target { get; set; }
            }
        }

        public static string ResourceTarget(string Context, string ResourceId)
        {
            return Context + "Resource/" + CalculateXXHash64(ResourceId);
        }

        private static string CalculateXXHash64(string key)
        {
            return Sparrow.Hashing.XXHash64.Calculate(key, System.Text.Encoding.UTF8).ToString();
        }
    }
}