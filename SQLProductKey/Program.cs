using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using SCOMAuthoringBook.Library.Helpers;

namespace SQLProductKey
{
  class Program
  {
    static void Main(string[] args)
    {
      string PrincipalName = ".";

      Console.WriteLine("1");

      if (!RegistryHelper.RegistryKeyExists(PrincipalName, "HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\Instance Names"))
      {
        Console.WriteLine("No SQL Server instances found.");
        return;
      }

      RegistryKey sqlRoot = RegistryHelper.GetRegistryKey(PrincipalName, "HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server");
      RegistryKey instanceListRoot = sqlRoot.OpenSubKey("Instance Names");
      foreach (string roleTypeKeyName in instanceListRoot.GetSubKeyNames())
      {
        RegistryKey roleTypeKey = instanceListRoot.OpenSubKey(roleTypeKeyName);
        foreach (string roleInstanceValueName in roleTypeKey.GetValueNames())
        {
          RegistryKey roleInstanceSetupKey = sqlRoot.OpenSubKey($"{(string)roleTypeKey.GetValue(roleInstanceValueName)}\\Setup");
          string versionStr = (string)roleInstanceSetupKey.GetValue("Version", "N/A");
          Version version = null;
          try { version = new Version(versionStr); } catch { }
          switch (roleTypeKeyName)
          {
            case "SQL":
              Console.WriteLine("Role Type: SQL Server");
              break;
            case "OLAP":
              Console.WriteLine("Role Type: SQL Server Analysis Services");
              break;
            case "RS":
              Console.WriteLine("Role Type: SQL Server Reporting Services");
              break;
            default:
              Console.WriteLine("Role Type: Other Services");
              break;
          }
          Console.WriteLine("Instance Name: " + roleInstanceValueName);
          Console.WriteLine("Version: {0} ({1})", versionStr, GetSQLGeneration(version));
          Console.WriteLine("Edition: " + (string)roleInstanceSetupKey.GetValue("Edition", "N/A"));
          Console.WriteLine("PatchLevel: " + (string)roleInstanceSetupKey.GetValue("PatchLevel", "N/A"));
          Console.WriteLine("Service Pack: " + (int)roleInstanceSetupKey.GetValue("SP", 0));
          string ProductKey = "N/A";
          try
          {
            if (version != null)
            {
              if (version.Major >= 11)
                ProductKey = RecoveryProductKeyFromBinary((byte[])roleInstanceSetupKey.GetValue("DigitalProductID", null));
              else if (version.Major >= 10)
              {
                byte[] productId = new byte[15];
                Array.Copy((byte[])roleInstanceSetupKey.GetValue("DigitalProductID", null), 52, productId, 0, 15);
                ProductKey = RecoveryProductKeyFromBinary(productId);
              }
              else
              {
                string template = "HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\{0}\\ProductID";
                for (int ver = 140; ver >= 70; ver = ver - 10)
                {
                  string regPath = string.Format(template, ver);
                  if (RegistryHelper.RegistryKeyExists(PrincipalName, regPath))
                  {
                    RegistryKey productKey = RegistryHelper.GetRegistryKey(PrincipalName, regPath);
                    foreach (string prdSubKey in productKey.GetValueNames())
                      if (prdSubKey.IndexOf("DigitalProductID") >= 0)
                      {
                        byte[] productId = new byte[15];
                        Array.Copy((byte[])productKey.GetValue(prdSubKey, null), 52, productId, 0, 15);
                        ProductKey = RecoveryProductKeyFromBinary(productId);
                      }
                  }
                }
              }
            }
          }
          catch (Exception e) { Console.WriteLine(e.Message); }
          Console.WriteLine("Product Key: " + ProductKey);
          Console.WriteLine();
        }
      }
    }

    private static string GetSQLGeneration(Version uniqueVersion)
    {
      if (uniqueVersion == null)
        return "Unknown";
      switch (uniqueVersion.Major)
      {
        // https://support.microsoft.com/en-nz/help/321185/how-to-determine-the-version-edition-and-update-level-of-sql-server-an
        case 7: return "7.0";
        case 8: return "2000";
        case 9: return "2005";
        case 10: if (uniqueVersion.Minor >= 50) return "2008 R2"; else return "2008";
        case 11: return "2012";
        case 12: return "2014";
        case 13: return "2016";
        case 14: return "2017";
        case 15: return "201?";
        default: return "Unsupported";
      }
    }

    private static string charsArray = "BCDFGHJKMPQRTVWXY2346789";

    private static string RecoveryProductKeyFromBinary(byte[] regBinary)
    {
      if (regBinary == null)
        return null;

      try
      {
        bool isNKey = ((regBinary[14] / 0x6) & 0x1) != 0;

        if (isNKey)
          regBinary[14] = Convert.ToByte((regBinary[14] & 0xF7));
        string regBinaryOutput = "";

        int last = 0;
        for (int i = 24; i >= 0; i--)
        {
          int k = 0;
          for (int j = 14; j >= 0; j--)
          {
            k = (k * 256) ^ regBinary[j]; // k = k * 256; k = regBinary[j] + k;
            regBinary[j] = Convert.ToByte(k / 24);
            k = k % 24;
          }
          regBinaryOutput = charsArray[k] + regBinaryOutput;
          last = k;
        }

        string regBinarypart1 = regBinaryOutput.Substring(1, last);
        string regBinarypart2 = regBinaryOutput.Substring(1, regBinaryOutput.Length - 1);
        if (isNKey)
          if (last == 0)
            regBinaryOutput = "N" + regBinarypart2;
          else
            regBinaryOutput = regBinarypart2.Insert(regBinarypart2.IndexOf(regBinarypart1) + regBinarypart1.Length, "N");

        string a = regBinaryOutput.Substring(0, 5);
        string b = regBinaryOutput.Substring(5, 5);
        string c = regBinaryOutput.Substring(10, 5);
        string d = regBinaryOutput.Substring(15, 5);
        string e = regBinaryOutput.Substring(20, 5);
        string regBinaryproduct = a + "-" + b + "-" + c + "-" + d + "-" + e;
        return regBinaryproduct;
      }
      catch
      {
        return null;
      }
    }
  }
}
