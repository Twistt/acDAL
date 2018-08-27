using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArachnidCreations.DevTools
{
	[System.AttributeUsage(System.AttributeTargets.Property | System.AttributeTargets.Field, AllowMultiple = true)]  // Multiuse attribute.
	public class DBPrimaryKey : System.Attribute
	{
		public DBPrimaryKey()
		{
		}
	}
}
