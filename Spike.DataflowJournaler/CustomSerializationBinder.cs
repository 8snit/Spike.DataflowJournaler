using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Spike.DataflowJournaler
{
    public class CustomSerializationBinder : DefaultSerializationBinder
    {
        private readonly IDictionary<string, Type> _nameToType = new Dictionary<string, Type>();

        private readonly IDictionary<Type, string> _typeToName = new Dictionary<Type, string>();

        public CustomSerializationBinder()
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (_typeToName.ContainsKey(type))
                    {
                        continue;
                    }

                    var customAttributes = type.GetCustomAttributes(typeof (JsonObjectAttribute), true);
                    foreach (var jsonObjectAttribute in customAttributes.OfType<JsonObjectAttribute>())
                    {
                        if (string.IsNullOrEmpty(jsonObjectAttribute.Id)
                            || _nameToType.ContainsKey(jsonObjectAttribute.Id))
                        {
                            continue;
                        }

                        _typeToName.Add(type, jsonObjectAttribute.Id);
                        _nameToType.Add(jsonObjectAttribute.Id, type);
                    }
                }
            }
        }

        public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            string name;
            if (_typeToName.TryGetValue(serializedType, out name))
            {
                assemblyName = null;
                typeName = name;
                return;
            }

            base.BindToName(serializedType, out assemblyName, out typeName);
        }

        public override Type BindToType(string assemblyName, string typeName)
        {
            if (string.IsNullOrEmpty(assemblyName))
            {
                Type type;
                if (_nameToType.TryGetValue(typeName, out type))
                {
                    return type;
                }
            }
            return base.BindToType(assemblyName, typeName);
        }
    }
}