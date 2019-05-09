using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;

namespace SCOMAuthoringBook.Library.Helpers
{
  class RegistryHelper
  {
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

    public static DateTime FromUnixTime(uint secondsSince1970)
    {
      DateTime Result = new DateTime(1970, 1, 1, 0, 0, 0, 0);
      return Result.AddSeconds(secondsSince1970);
    }
  }
}
