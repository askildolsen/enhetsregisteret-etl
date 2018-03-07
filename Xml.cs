using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Xml;
using System.Xml.Linq;

namespace enhetsregisteret_etl
{
    class Xml
    {
        public static IEnumerable<ExpandoObject> ExpandoStream(WebRequest request)
        {
            ((HttpWebRequest)request).Accept = "application/xml";

            using(var response = request.GetResponse())
            using(var stream = response.GetResponseStream())
            {
                foreach (var stotte in XDocument.Load(stream).Root.Elements())
                {
                    dynamic expando = new ExpandoObject();
                    var expandoDic = (IDictionary<string, object>)expando;

                    foreach(var kv in stotte.Elements().Where(x => !String.IsNullOrEmpty(x.Value)).ToDictionary(x => x.Name.LocalName, x => (object)x.Value))
                    {
                        expandoDic.Add(kv);
                    }

                    yield return expando;         
                }
            }
        }
    }
}
