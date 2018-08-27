using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArachnidCreations.DevTools
{
    // Multiuse attribute.
    [System.AttributeUsage(System.AttributeTargets.Class | System.AttributeTargets.Interface, AllowMultiple = false)]  // Multiuse attribute.
    public class DBTable : System.Attribute
    {
        string name;
        public DBTable(string name)
        {
            this.name = name;
        }

        public string GetName()
        {
            return name;
        }
    }
}
