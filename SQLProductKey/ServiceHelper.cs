using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Win32;
#if ServiceHelperWMI
using System.Management;
using static System.Management.ManagementObjectCollection;
#endif

namespace SQLProductKey
{
  public static class ServiceHelper
  {
    /// <summary>
    /// Translate a wildcard sting to a regular expression one.
    /// </summary>
    /// <param name="pattern">Wildcard expression to translate</param>
    /// <returns>Regular expression equivalent of the given wildcard one</returns>
    public static string WildcardToRegex(string pattern)
    {
      return "^" + Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".") + "$";
    }

    public static bool IsRegexMatchAny(string input, IEnumerable<string> patterns)
    {
      foreach (var pattern in patterns)
        if (Regex.IsMatch(input, pattern))
          return true;
      return false;
    }

    public static string ReadRegistryString(string computerName, string fullPath, string defaultValue = null)
    {
      return ReadRegistryValue(computerName, fullPath, defaultValue).ToString();
    }

    public static DateTime ReadRegistryUnixTime(string computerName, string fullPath)
    {
      int rawResult = (int)ReadRegistryValue(computerName, fullPath, null);
      uint unixTime = unchecked((uint)rawResult);
      return FromUnixTime(unixTime);
    }

    public static uint ReadRegistryUInt(string computerName, string fullPath, uint? defaultValue = null)
    {
      int rawResult = (int)ReadRegistryValue(computerName, fullPath, defaultValue);
      return unchecked((uint)rawResult);
    }

    public static bool ReadRegistryBoolean(string computerName, string fullPath, bool? defaultValue = null)
    {
      int? defaultInt = null;
      if (defaultValue != null)
        defaultInt = defaultValue == true ? 1 : 0;
      int RawResult = (int)ReadRegistryValue(computerName, fullPath, defaultInt);
      return RawResult != 0;
    }

    public static ulong ReadRegistryULong(string computerName, string fullPath, ulong? defaultValue = null)
    {
      return unchecked((ulong)ReadRegistryValue(computerName, fullPath, defaultValue));
    }

    /// <summary>
    /// Read registry parameter value from full path: HKLM:\Key\Parameter. The PowerShell-style key prefix can be replaced with its
    /// full name: HKEY_LOCAL_MACHINE
    /// </summary>
    /// <param name="computerName">Remote computer name. Leave empty, null, '.' or 'localhost' to access local Registry.</param>
    /// <param name="fullPath"></param>
    /// <returns></returns>
    public static object ReadRegistryValue(string computerName, string fullPath, object defaultValue = null)
    {
      try
      {
        int lastPeriodPos = fullPath.LastIndexOf("\\");
        string pathName = fullPath.Substring(0, lastPeriodPos);
        RegistryKey valueKey = GetRegistryKey(computerName, pathName);

        if (valueKey == null)
          throw new IOException(pathName + " registry path not found.");
        string valueName = fullPath.Substring(lastPeriodPos + 1);
        object returnValue = valueKey.GetValue(valueName);
        if (returnValue == null)
          throw new IOException(valueName + " registry value not found.");
        return returnValue;
      }
      catch (IOException)
      {
        if (defaultValue != null)
          return defaultValue;
        else
          throw;
      }
    }

    public static bool RegistryKeyExists(string computerName, string keyPath)
    {
      if (GetRegistryKey(computerName, keyPath) == null)
        return false;
      else
        return true;
    }

    public static bool RegistryValueExists(string computerName, string fullPath)
    {
      try
      {
        ReadRegistryValue(computerName, fullPath);
        return true;
      }
      catch (IOException)
      {
        return false;
      }
    }

    public static RegistryKey GetRegistryKey(string computerName, string keyPath)
    {
      int firstPeriodPos = keyPath.IndexOf("\\");
      string hKeyName = keyPath.Substring(0, firstPeriodPos);
      RegistryHive hKey = RegistryHive.LocalMachine;
      switch (hKeyName.ToUpperInvariant())
      {
        case "HKLM:":
        case "HKLM":
        case "HKEY_LOCAL_MACHINE":
          hKey = RegistryHive.LocalMachine;
          break;
        case "HKCR:":
        case "HKCR":
        case "HKEY_CLASSES_ROOT":
          hKey = RegistryHive.ClassesRoot;
          break;
        case "HKCC:":
        case "HKCC":
        case "HKEY_CURRENT_CONFIG":
          hKey = RegistryHive.CurrentConfig;
          break;
        case "HKCU:":
        case "HKCU":
        case "HKEY_CURRENT_USER":
          hKey = RegistryHive.CurrentUser;
          break;
        case "HKDD:":
        case "HKDD":
        case "HKEY_DYN_DATA":
          hKey = RegistryHive.DynData;
          break;
        case "HKPD:":
        case "HKPD":
        case "HKEY_PERFROMANCE_DATA":
          hKey = RegistryHive.PerformanceData;
          break;
        case "HKU:":
        case "HKU":
        case "HKEY_USERS":
          hKey = RegistryHive.Users;
          break;
      }
      RegistryKey remoteRegistry = RegistryKey.OpenRemoteBaseKey(hKey, LocalizeName(computerName));
      string pathName = keyPath.Substring(firstPeriodPos + 1);
      if (string.IsNullOrEmpty(pathName))
        return remoteRegistry;
      else
        return remoteRegistry.OpenSubKey(pathName);
    }

    public static string ObjectArrayToSeparatedString(IEnumerable<object> inputObjects, string separator = "; ")
    {
      string Result = "";
      foreach (object obj in inputObjects)
        Result += obj.ToString() + separator;
      return Result.Substring(0, Result.LastIndexOf(separator));
    }

    /// <summary>
    /// Simplify computer name if it is the local computer. Make the string empty (default) or replace with the specified value.
    /// </summary>
    /// <param name="remoteName">Input: computer name to match with the local.</param>
    /// <param name="localReplacement">If the input name is local, then replace with this string, default to empty string.</param>
    /// <returns></returns>
    public static string LocalizeName(string remoteName, string localReplacement = "")
    {
      if (string.IsNullOrEmpty(remoteName) || remoteName == "." || remoteName == string.Empty || remoteName.ToUpperInvariant() == "LOCALHOST")
        return localReplacement;
      if (remoteName.ToUpperInvariant() == Environment.MachineName.ToUpperInvariant())
        return localReplacement;
      if (remoteName.ToUpperInvariant() == Dns.GetHostName().ToUpperInvariant())
        return localReplacement;
      if (remoteName.ToUpperInvariant() == Dns.GetHostName().ToUpperInvariant() + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName.ToUpperInvariant())
        return localReplacement;
      return remoteName;
    }

    public static string GetMachineDNSName()
    {
      string computerName = "";
      if (string.IsNullOrEmpty(Dns.GetHostName()))
        computerName = Environment.MachineName;
      else
      {
        if (Dns.GetHostName().IndexOf(".") >= 0)
          computerName = Dns.GetHostName();
        else
          computerName = Dns.GetHostName() + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;
      }
      return computerName;
    }

    public static bool IsCluster(string computerName)
    {
      RegistryKey remoteRegistry = RegistryKey.OpenRemoteBaseKey(RegistryHive.LocalMachine, LocalizeName(computerName));
      var clusterKey = remoteRegistry.OpenSubKey("Cluster");
      return clusterKey != null;
    }

    public static bool Is64Bit()
    {
      string processorArchitecture = null;
      try
      {
        processorArchitecture = Environment.GetEnvironmentVariable("PROCESSOR_ARCHITECTURE");
      }
      catch
      {
        return false;
      }
      if (string.IsNullOrEmpty(processorArchitecture))
        return false;
      if (processorArchitecture.Contains("AMD64"))
        return true;
      return false;
    }

    public static string GetTimeBasedTempFileName(bool useDate, bool useTime, bool useSeconds, bool useMilliseconds, bool useRandom, string prefix = null, string extension = "log", bool useLocalTime = true)
    {
      string fileName = "";
      DateTime now = DateTime.UtcNow;
      if (useLocalTime)
        now = DateTime.Now;
      if (useDate)
        fileName += now.ToString("yyyy-MM-dd");
      if (useTime)
        fileName += (useDate ? "-" : "") + now.ToString("HH-mm");
      if (useSeconds)
        fileName+= (useDate || useTime ? "-" : "") + now.ToString("ss");
      if (useMilliseconds)
        fileName += (useDate || useTime || useSeconds ? "-" : "") + now.Millisecond.ToString();
      if (useRandom)
      {
        Random random = new Random();
        fileName += (useDate || useTime || useSeconds || useMilliseconds ? "-" : "") + random.Next().ToString("D4").Substring(0, 3);
      }
      fileName = (prefix ?? "") + fileName + "." + (extension ?? "").TrimStart(new char[] { ' ', '.' });
      return fileName;
    }

#if ServiceHelperWMI
    public static string GetMachinePrincipalName()
    {
      string Result = ".";
      try
      {
        ManagementScope localWMI = new ManagementScope(ManagementPath.DefaultPath);
        localWMI.Connect();
        ObjectQuery query = new ObjectQuery("SELECT * FROM Win32_ComputerSystem");
        ManagementObjectSearcher computerSystemSearcher = null;
        ManagementObjectCollection computerSystemList = null;
        ManagementObjectEnumerator computerSystemEnum = null;
        try
        {
          computerSystemSearcher = new ManagementObjectSearcher(localWMI, query);
          computerSystemList = computerSystemSearcher.Get();
          computerSystemEnum = computerSystemList.GetEnumerator();
          if (computerSystemEnum.MoveNext())
          {
            var computerSystem = computerSystemEnum.Current;
            if ((bool)computerSystem["PartOfDomain"])
              Result = computerSystem["DNSHostName"].ToString() + "." + computerSystem["Domain"].ToString();
            else
              Result = computerSystem["DNSHostName"].ToString();
          }
          else
            throw new ArgumentNullException("No instances of Win32_ComputerSystem WMI class returned.");
        }
        finally
        {
          if (computerSystemEnum != null)
            computerSystemEnum.Dispose();
          if (computerSystemList != null)
            computerSystemList.Dispose();
          if (computerSystemSearcher != null)
            computerSystemSearcher.Dispose();
        }
      }
      catch
      {
        Result = Environment.MachineName;
      }
      return Result;
    }

    public static long GetUptime(string computerName)
    {
      ManagementObject mo = new ManagementObject(@"\\" + LocalizeName(computerName, ".") + @"\root\cimv2:Win32_OperatingSystem=@");
      DateTime lastBootUp = ManagementDateTimeConverter.ToDateTime(mo["LastBootUpTime"].ToString());
      return Convert.ToInt64((DateTime.Now.ToUniversalTime() - lastBootUp.ToUniversalTime()).TotalSeconds);
    }

    public static DataTable GetQueryWMI(string computerName, string WQLquery, string WMInamespace = @"\root\cimv2")
    {
      ManagementScope scope = new ManagementScope("\\\\" + LocalizeName(computerName, ".") + WMInamespace);
      scope.Connect();
      if (scope.IsConnected)
      {
        ObjectQuery query = new ObjectQuery(WQLquery);
        ManagementObjectSearcher objectList = null;
        ManagementObjectCollection allResources = null;
        DataTable Result = null;
        try
        {
          objectList = new ManagementObjectSearcher(scope, query);
          allResources = objectList.Get();
          foreach (ManagementBaseObject x in allResources)
          {
            // create output table and use the first row to initialize columns
            if (Result == null)
            {
              
              Result = new DataTable(x.ClassPath.ClassName);
              foreach (var col in x.Properties)
                Result.Columns.Add(col.Name, col.GetManagedType());
            }
            // Add data row
            DataRow newRow = Result.NewRow();
            foreach (var dataCol in x.Properties)
            {
              if (x[dataCol.Name] == null)
              {
                newRow[dataCol.Name] = DBNull.Value;
                continue;
              }
              if (dataCol.Type == CimType.DateTime)
                newRow[dataCol.Name] = ManagementDateTimeConverter.ToDateTime(x[dataCol.Name].ToString());
              else
                newRow[dataCol.Name] = x[dataCol.Name];
            }
            Result.Rows.Add(newRow);
            x.Dispose();
          }
        }
        finally
        {
          if (allResources != null)
            allResources.Dispose();
          if (objectList != null)
            objectList.Dispose();
        }
        return Result;
      }
      return null;
    }
#endif

    public static DateTime FromUnixTime(uint secondsSince1970)
    {
      DateTime Result = new DateTime(1970, 1, 1, 0, 0, 0, 0);
      return Result.AddSeconds(secondsSince1970);
    }
  }

  /// <summary>
  /// Compare machine names taking domain suffix in account. If both names are FQDN or NetBIOS names, then literal comparison
  /// applied, otherwise either name reduced to NetBIOS format.
  /// </summary>
  public class MachineNameComparer : IComparer<string>
  {
    public int Compare(string x, string y)
    {
      bool isXfull = (x.IndexOf(".") > 0); // cannot be 0, as ".dnssuffix.name" is invalid.
      bool isYfull = (y.IndexOf(".") > 0);
      if (isXfull && isYfull)
        return StringComparer.OrdinalIgnoreCase.Compare(x, y);
      if (!isXfull && !isYfull)
        return StringComparer.OrdinalIgnoreCase.Compare(x, y);
      if (isXfull)
        return StringComparer.OrdinalIgnoreCase.Compare(x.Substring(0, x.IndexOf(".")), y);
      if (isYfull)
        return StringComparer.OrdinalIgnoreCase.Compare(x, y.Substring(0, y.IndexOf(".")));
      throw new Exception("It's impossible to get here. Suppressing compiler error.");
    }

    public static MachineNameComparer OrdinalIgnoreCase
    {
      get { return new MachineNameComparer(); }
    }
  }

  #if ServiceHelperWMI
  public static class PropertyDataExtension
  {
    public static Type GetManagedType(this PropertyData property)
    {
      if (property.IsArray)
      {
        switch (property.Type)
        {
          case CimType.Boolean: return typeof(bool[]);
          case CimType.Char16: return typeof(char[]);
          case CimType.DateTime: return typeof(DateTime[]);
          case CimType.Real32: return typeof(float[]);
          case CimType.Real64: return typeof(double[]);
          case CimType.SInt16: return typeof(short[]);
          case CimType.SInt32: return typeof(int[]);
          case CimType.SInt64: return typeof(long[]);
          case CimType.SInt8: return typeof(sbyte[]);
          case CimType.String: return typeof(string[]);
          case CimType.UInt16: return typeof(ushort[]);
          case CimType.UInt32: return typeof(uint[]);
          case CimType.UInt64: return typeof(ulong[]);
          case CimType.UInt8: return typeof(byte[]);
          default: return typeof(object[]);
        }
      }
      else
      {
        switch (property.Type)
        {
          case CimType.Boolean: return typeof(bool);
          case CimType.Char16: return typeof(char);
          case CimType.DateTime: return typeof(DateTime);
          case CimType.Real32: return typeof(float);
          case CimType.Real64: return typeof(double);
          case CimType.SInt16: return typeof(short);
          case CimType.SInt32: return typeof(int);
          case CimType.SInt64: return typeof(long);
          case CimType.SInt8: return typeof(sbyte);
          case CimType.String: return typeof(string);
          case CimType.UInt16: return typeof(ushort);
          case CimType.UInt32: return typeof(uint);
          case CimType.UInt64: return typeof(ulong);
          case CimType.UInt8: return typeof(byte);
          default: return typeof(object);
        }
      }
    }
  }
#endif
}