using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArachnidCreations.DevTools
{
    // Multiuse attribute.
    [System.AttributeUsage(System.AttributeTargets.Property | System.AttributeTargets.Field, AllowMultiple = true)]  // Multiuse attribute.
    public class DBColumnIgnoreOnUpdate : System.Attribute
    {
        /// <summary>
        /// This is intended to mark columns that you want want serialized into SQL through the ORM Such as null dates.
        /// </summary>
        public DBColumnIgnoreOnUpdate()
        {
        }

    }

}
