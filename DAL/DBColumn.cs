using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArachnidCreations.DevTools
{
    // Multiuse attribute.
    [System.AttributeUsage(System.AttributeTargets.Property | System.AttributeTargets.Field, AllowMultiple = true)]  // Multiuse attribute.
    public class DBColumn : System.Attribute
    {
        string name;
        public DBColumn(string name)
        {
            this.name = name;
        }

        public string GetName()
        {
            return name;
        }
    }
}
