using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Library.Common;

namespace Library.DatabasePersistentObjects
{
  public class DBQueue<T> : DBQueue
  {
    public DBQueue(SqlConnection sqlConnection, string tableName) : base(sqlConnection, tableName)
    {
    }

    public ResultWrapper<long> TrySubmitMessage(string messageType, T message, DateTime? deliveryAfterUTC = null)
    {
      try
      {
        return TrySubmitMessage(messageType, JsonConvert.SerializeObject(message), deliveryAfterUTC);
      }
      catch (Exception e)
      {
        return new ResultWrapper<long>(e, FailureReason.SystemError);
      }
    }
  }

  public class DBQueue : IDisposable
  {
    protected enum MessageStatus { Submitted = 0, PickedForProcessing = 1, Failed = 2, Completed = 3, Waiting = 4, PickedForParallelProcessing = 5 }

    protected SqlConnection refConnection;
    protected string tableName;

    public DBQueue(SqlConnection sqlConnection, string tableName)
    {
      refConnection = sqlConnection;
      if (refConnection.State != ConnectionState.Open)
      {
        refConnection.Open();
      }
      this.tableName = tableName;
      PrepareTable();
    }

    public ResultWrapper<long> TrySubmitMessage(string messageType, string message, DateTime? deliveryAfterUTC = null)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<long>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<long>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        string insertSQL = $"insert into DBQueue.{tableName}(SubmitDate, MessageType, MessageBody, StatusDate, StatusExpirationDate, MessageStatus) output INSERTED.MessageId values (GETUTCDATE(), @MessageType, @MessageBody, GETUTCDATE(), @StatusExpirationDate, @MessageStatus)";
        using (SqlCommand insertCommand = new SqlCommand(insertSQL, refConnection))
        {
          insertCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
          insertCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)message ?? DBNull.Value;
          insertCommand.Parameters.Add("@MessageStatus", SqlDbType.Int).Value = deliveryAfterUTC == null ? (int)MessageStatus.Submitted : MessageStatus.Waiting;
          insertCommand.Parameters.Add("@StatusExpirationDate", SqlDbType.DateTime).Value = (object)deliveryAfterUTC ?? DBNull.Value;
          return new ResultWrapper<long>((long)insertCommand.ExecuteScalar());
        }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<long>(e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<long>(e, FailureReason.SystemError);
      }
    }

    public ResultWrapper<Tuple<long, string>> TryPeekMessageForParallelProcessing(string messageType, string instanceId, int messageTimeoutSeconds)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<Tuple<long, string>>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (string.IsNullOrWhiteSpace(instanceId))
          return new ResultWrapper<Tuple<long, string>>(new ArgumentNullException("instanceId", "instanceId argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<Tuple<long, string>>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);
        if (instanceId.Length > 250)
          return new ResultWrapper<Tuple<long, string>>(new ArgumentOutOfRangeException("instanceId", "instanceId argument must not exceed 250 characters."), FailureReason.ApplicationError);

        using (SqlTransaction transaction = refConnection.BeginTransaction())
          try
          {
            // if base message is either: submitted, ready, or already in parallel processing
            // Selecting ALL messages to avoid blocking
            string selectSQL = $"select MessageId, MessageBody from DBQueue.{tableName} WITH (XLOCK, HOLDLOCK) where ((MessageStatus = {((int)MessageStatus.Submitted).ToString()}) or (MessageStatus = {((int)MessageStatus.PickedForParallelProcessing).ToString()}) or (MessageStatus = {((int)MessageStatus.Waiting).ToString()} and StatusExpirationDate <= GETUTCDATE())) and (MessageType = '{messageType}')";
            List<Tuple<long, string>> topMessages = new List<Tuple<long, string>>();
            using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection, transaction))
            {
              using (SqlDataReader reader = selectCommand.ExecuteReader())
              {
                if (reader.HasRows)
                  while (reader.Read())
                    topMessages.Add(new Tuple<long, string>(reader.GetInt64(0), reader.GetString(1)));
                reader.Close();
              }
            }
            if (topMessages != null && topMessages.Count > 0)
              foreach (Tuple<long, string> topMessage in topMessages)
              {
                // check if parallel instance already exists for selected top level message
                string parallelSelectSQL = $"select MessageStatus, StatusExpirationDate from DBQueue.{tableName}ParallelStatus WITH (XLOCK, HOLDLOCK) where MessageId = {topMessage.Item1.ToString()} and InstanceId = '{instanceId}'";
                int parallelMessageStatus = -1; // no parallel status exists
                DateTime parallelMessageStatusExpirationDate = DateTime.MaxValue;
                using (SqlCommand parallelSelectCommand = new SqlCommand(parallelSelectSQL, refConnection, transaction))
                {
                  using (SqlDataReader parallelReader = parallelSelectCommand.ExecuteReader())
                  {
                    if (parallelReader.HasRows && parallelReader.Read())
                    {
                      parallelMessageStatus = parallelReader.GetInt32(0);
                      parallelMessageStatusExpirationDate = parallelReader.GetDateTime(1);
                    }
                    parallelReader.Close();
                  }
                }
                // parallel status already exists
                if (parallelMessageStatus >= 0)
                {
                  // picked, but expired
                  if (parallelMessageStatus == (int)MessageStatus.PickedForParallelProcessing && parallelMessageStatusExpirationDate <= DateTime.UtcNow)
                  {
                    // update
                    string updateParallelStatusSQL = $"update DBQueue.{tableName}ParallelStatus set MessageStatus = {((int)MessageStatus.PickedForParallelProcessing).ToString()}, StatusDate = GETUTCDATE(), StatusExpirationDate = DATEADD(SECOND, {messageTimeoutSeconds.ToString()}, GETUTCDATE()) where MessageId = {topMessage.Item1.ToString()} and InstanceId = '{instanceId}'";
                    using (SqlCommand updateParallelCommand = new SqlCommand(updateParallelStatusSQL, refConnection, transaction))
                      updateParallelCommand.ExecuteNonQuery();
                    transaction.Commit();
                    return new ResultWrapper<Tuple<long, string>>(topMessage);
                  }
                  // else => still in processing, or completed, or failed, go to next top message
                }
                // no parralel status exists for this top message and parallel instance
                else
                {
                  // set status to parallel processing for the top message
                  string updateSQL = $"update DBQueue.{tableName} set MessageStatus={((int)MessageStatus.PickedForParallelProcessing).ToString()}, StatusDate = GETUTCDATE(), StatusExpirationDate = NULL where MessageId={topMessage.Item1.ToString()}";
                  using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection, transaction))
                  {
                    updateCommand.ExecuteNonQuery();
                  }
                  // insert new parallel status
                  string insertSQL = $"insert into DBQueue.{tableName}ParallelStatus (MessageId, InstanceId, StatusDate, StatusExpirationDate, MessageStatus) values ({topMessage.Item1.ToString()}, '{instanceId}', GETUTCDATE(), DATEADD(SECOND, {messageTimeoutSeconds.ToString()}, GETUTCDATE()), {((int)MessageStatus.PickedForParallelProcessing).ToString()})";
                  using (SqlCommand insertCommand = new SqlCommand(insertSQL, refConnection, transaction))
                    insertCommand.ExecuteNonQuery();
                  transaction.Commit();
                  return new ResultWrapper<Tuple<long, string>>(topMessage);
                }
                // continue to net top message
              }

            // if no messages available for peeking
            transaction.Rollback();
            return new ResultWrapper<Tuple<long, string>>(FailureReason.NotFound);
          }
          catch (SqlException e)
          {
            transaction.Rollback();
            refConnection.Close();
            refConnection.Open();
            return new ResultWrapper<Tuple<long, string>>(e, FailureReason.ApplicationError);
          }
          catch (Exception e)
          {
            transaction.Rollback();
            refConnection.Close();
            refConnection.Open();
            return new ResultWrapper<Tuple<long, string>>(e, FailureReason.SystemError);
          }
      }
      catch (Exception e2)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<Tuple<long, string>>(e2, FailureReason.SystemError);
      }
    }

    public ResultWrapper<Tuple<long, string>> TryPeekMessageForProcessing(string messageType, int messageTimeoutSeconds)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<Tuple<long, string>>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<Tuple<long, string>>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        using (SqlTransaction transaction = refConnection.BeginTransaction())
          try
          {
            string selectSQL = $"select top 1 MessageId, MessageBody from DBQueue.{tableName} WITH (XLOCK, HOLDLOCK) where ((MessageStatus = {((int)MessageStatus.Submitted).ToString()}) or (MessageStatus = {((int)MessageStatus.PickedForProcessing).ToString()} and StatusExpirationDate < GETUTCDATE()) or (MessageStatus = {((int)MessageStatus.Waiting).ToString()} and StatusExpirationDate <= GETUTCDATE())) and (MessageType = '{messageType}')";
            using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection, transaction))
            {
              Tuple<long, string> result = null;
              using (SqlDataReader reader = selectCommand.ExecuteReader())
              {
                if (reader.HasRows && reader.Read())
                  result = new Tuple<long, string>(reader.GetInt64(0), reader.GetString(1));
                reader.Close();
              }
              if (result != null)
              {
                string updateSQL = $"update DBQueue.{tableName} set MessageStatus={((int)MessageStatus.PickedForProcessing).ToString()}, StatusDate = GETUTCDATE(), StatusExpirationDate = DATEADD(SECOND, {messageTimeoutSeconds.ToString()}, GETUTCDATE()) where MessageId={result.Item1.ToString()}";
                using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection, transaction))
                {
                  updateCommand.ExecuteNonQuery();
                }
                transaction.Commit();
                return new ResultWrapper<Tuple<long, string>>(result);
              }
            }
            // if no messages available for peeking
            transaction.Rollback();
            return new ResultWrapper<Tuple<long, string>>(FailureReason.NotFound);
          }
          catch (SqlException e)
          {
            transaction.Rollback();
            refConnection.Close();
            refConnection.Open();
            return new ResultWrapper<Tuple<long, string>>(e, FailureReason.ApplicationError);
          }
          catch (Exception e)
          {
            transaction.Rollback();
            refConnection.Close();
            refConnection.Open();
            return new ResultWrapper<Tuple<long, string>>(e, FailureReason.SystemError);
          }
      }
      catch (Exception e2)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<Tuple<long, string>>(e2, FailureReason.SystemError);
      }
    }

    protected ResultWrapper<bool> TrySetMessageParallelStatus(long MessageId, MessageStatus messageStatus, string instanceId)
    {
      try
      {
        string updateSQL = $"update DBQueue.{tableName}ParallelStatus set MessageStatus = {((int)messageStatus).ToString()}, StatusDate = GETUTCDATE() where MessageId = {MessageId.ToString()} and InstanceId = '{instanceId}'";
        using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection))
          updateCommand.ExecuteNonQuery();
        return new ResultWrapper<bool>(true);
      }
      catch (Exception e)
      {
        return new ResultWrapper<bool>(false, e, FailureReason.SystemError);
      }
    }

    public ResultWrapper<bool> TrySetMessageParallelFailed(long MessageId, string instanceId)
    {
      return TrySetMessageParallelStatus(MessageId, MessageStatus.Failed, instanceId);
    }

    public ResultWrapper<bool> TrySetMessageParallelCompleted(long MessageId, string instanceId)
    {
      return TrySetMessageParallelStatus(MessageId, MessageStatus.Completed, instanceId);
    }

    protected ResultWrapper<bool> TrySetMessageStatus(long MessageId, MessageStatus messageStatus, string messageStatusMessage)
    {
      try
      {
        using (SqlTransaction transaction = refConnection.BeginTransaction())
          try
          {
            string selectSQL = $"select SubmitDate, MessageType, MessageBody from DBQueue.{tableName} WITH (XLOCK, HOLDLOCK) where MessageId = {MessageId.ToString()}";
            DateTime SubmitDate;
            string MessageType, MessageBody;
            using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection, transaction))
            {
              using (SqlDataReader reader = selectCommand.ExecuteReader())
              {
                reader.Read();
                SubmitDate = reader.GetDateTime(0);
                MessageType = reader.GetString(1);
                MessageBody = reader.IsDBNull(2) ? null : reader.GetString(2);
                reader.Close();
              }
            }
            string archiveSQL = $"insert into DBQueue.{tableName}Archive (MessageId, SubmitDate, MessageType, MessageBody, StatusDate, StatusExpirationDate, MessageStatus, MessageStatusMessage) values (@MessageId, @SubmitDate, @MessageType, @MessageBody, GETUTCDATE(), NULL, @MessageStatus, @MessageStatusMessage)";
            using (SqlCommand insertCommand = new SqlCommand(archiveSQL, refConnection, transaction))
            {
              insertCommand.Parameters.Add("@MessageId", SqlDbType.BigInt).Value = MessageId;
              insertCommand.Parameters.Add("@SubmitDate", SqlDbType.DateTime2).Value = SubmitDate;
              insertCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = MessageType;
              insertCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)MessageBody ?? DBNull.Value;
              insertCommand.Parameters.Add("@MessageStatus", SqlDbType.Int).Value = (int)messageStatus;
              insertCommand.Parameters.Add("@MessageStatusMessage", SqlDbType.NVarChar, 250).Value = (object)messageStatusMessage ?? DBNull.Value;
              insertCommand.ExecuteNonQuery();
            }
            string deleteSQL = $"delete from DBQueue.{tableName} where MessageId={MessageId.ToString()}";
            using (SqlCommand deleteCommand = new SqlCommand(deleteSQL, refConnection, transaction))
              deleteCommand.ExecuteNonQuery();
            transaction.Commit();
            return new ResultWrapper<bool>(true);
          }
          catch (Exception e)
          {
            transaction.Rollback();
            return new ResultWrapper<bool>(false, e, FailureReason.SystemError);
          }
      }
      catch (Exception e)
      {
        return new ResultWrapper<bool>(false, e, FailureReason.SystemError);
      }
    }

    public ResultWrapper<bool> TrySetMessageFailed(long MessageId, string messageStatusMessage)
    {
      return TrySetMessageStatus(MessageId, MessageStatus.Failed, messageStatusMessage);
    }

    public ResultWrapper<bool> TrySetMessageCompleted(long MessageId, string messageStatusMessage)
    {
      return TrySetMessageStatus(MessageId, MessageStatus.Completed, messageStatusMessage);
    }

    public ResultWrapper<bool> TrySetMessageTimeout(ulong MessageId, int newTimeoutSeconds)
    {
      try
      {
        string updateSQL = $"update DBQueue.{tableName} set StatusExpirationDate = DATEADD(SECOND, {newTimeoutSeconds.ToString()}, GETUTCDATE()) where MessageId={MessageId.ToString()}";
        using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection))
        {
          updateCommand.ExecuteNonQuery();
        }
        return new ResultWrapper<bool>(true);
      }
      catch (Exception e)
      {
        return new ResultWrapper<bool>(false, e, FailureReason.SystemError);
      }
    }

    private void PrepareTable()
    {
      RunSQLCommand($"IF not exists (select * from sys.schemas WHERE name = 'DBQueue') BEGIN EXEC('CREATE SCHEMA [DBQueue]') END", refConnection);
      RunSQLCommand(@"IF not exists (select * from sys.tables t join sys.schemas s on s.schema_id=t.schema_id where t.name = '" + tableName + @"' and s.name = 'DBQueue')
BEGIN
  CREATE TABLE DBQueue." + tableName + @"
  (
    MessageId bigint IDENTITY(1,1) not null primary key,
    SubmitDate DATETIME2,
    MessageType NVARCHAR(250),
    MessageBody NVARCHAR(MAX),
    StatusDate DATETIME2,
    StatusExpirationDate DATETIME2,
    MessageStatus int
  )
END", refConnection);
      RunSQLCommand(@"IF not exists (select * from sys.tables t join sys.schemas s on s.schema_id=t.schema_id where t.name = '" + tableName + "Archive" + @"' and s.name = 'DBQueue')
BEGIN
  CREATE TABLE DBQueue." + tableName + "Archive" + @"
  (
    MessageId bigint not null primary key,
    SubmitDate DATETIME2,
    MessageType NVARCHAR(250),
    MessageBody NVARCHAR(MAX),
    StatusDate DATETIME2,
    StatusExpirationDate DATETIME2,
    MessageStatus int,
    MessageStatusMessage NVARCHAR(250)
  )
END", refConnection);
      RunSQLCommand(@"IF not exists (select * from sys.tables t join sys.schemas s on s.schema_id=t.schema_id where t.name = '" + tableName + "ParallelStatus" + @"' and s.name = 'DBQueue')
BEGIN
  CREATE TABLE DBQueue." + tableName + "ParallelStatus" + @"
  (
    MessageId bigint,
    InstanceId NVARCHAR(250) not null,
    StatusDate DATETIME2,
    StatusExpirationDate DATETIME2,
    MessageStatus int,
    constraint FK_" + tableName + @"_Cascade foreign key (MessageId) references DBQueue." + tableName + @"(MessageId) on delete cascade,
    constraint UN_" + tableName + @"_Unique unique(MessageId, InstanceId)
  )
END", refConnection);
    }

    protected static void RunSQLCommand(string command, SqlConnection connection)
    {
      using (SqlCommand sqlCommand = new SqlCommand(command, connection))
      {
        sqlCommand.ExecuteNonQuery();
      }
    }

    public void Dispose()
    {
      refConnection.Dispose();
    }
  }

  public delegate string CacheEntryUpdateCallback(string inputEntry);

  public delegate bool JsonConditionInspector(JToken jToken);

  public class DBCache : IDisposable
  {
    protected SqlConnection refConnection;
    protected string tableName;

    public DBCache(SqlConnection sqlConnection, string tableName)
    {
      refConnection = sqlConnection;
      DefaultConstructor(tableName);
    }

    public DBCache(string databaseConnectionString, string tableName)
    {
      refConnection = new SqlConnection(databaseConnectionString);
      DefaultConstructor(tableName);
    }

    private void DefaultConstructor(string tableName)
    {
      if (refConnection.State != ConnectionState.Open)
      {
        refConnection.Open();
      }
      this.tableName = tableName;
      PrepareTable();
    }

    public ResultWrapper<IList<Guid>> TryListEntries(string messageType, string whereCondition)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<IList<Guid>>(new List<Guid>(), new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<IList<Guid>>(new List<Guid>(), new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        // TODO: Avoid string query manipulation
        string selectSQL = $"select EntryId, ExpirationDate from DBCache.{tableName} where MessageType = '{messageType}' " + (string.IsNullOrWhiteSpace(whereCondition) ? "" : (" and (" + whereCondition + ")"));
        using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection))
        {
          using (SqlDataReader reader = selectCommand.ExecuteReader())
            if (reader.HasRows)
            {
              List<Guid> result = new List<Guid>();
              while (reader.Read())
              {
                // not expired
                if (reader.IsDBNull(1) || (!reader.IsDBNull(1) && reader.GetDateTime(1) > DateTime.UtcNow))
                  result.Add(reader.GetGuid(0));
              }
              return new ResultWrapper<IList<Guid>>(result);
            }
            else
            {
              return new ResultWrapper<IList<Guid>>(new List<Guid>());
            }
        }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<IList<Guid>>(new List<Guid>(), e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<IList<Guid>>(new List<Guid>(), e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<IList<Guid>> TryListEntries(string messageType, JsonConditionInspector whereCondition)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<IList<Guid>>(new List<Guid>(), new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<IList<Guid>>(new List<Guid>(), new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        string selectSQL = $"select EntryId, ExpirationDate, MessageBody from DBCache.{tableName} where MessageType = '{messageType}'";
        using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection))
        {
          using (SqlDataReader reader = selectCommand.ExecuteReader())
            if (reader.HasRows)
            {
              List<Guid> result = new List<Guid>();
              while (reader.Read())
              {
                // not expired
                if (reader.IsDBNull(1) || (!reader.IsDBNull(1) && reader.GetDateTime(1) > DateTime.UtcNow))
                {
                  JToken message = JToken.Parse(reader.GetString(2));
                  if (whereCondition(message))
                    result.Add(reader.GetGuid(0));
                }
              }
              return new ResultWrapper<IList<Guid>>(result);
            }
            else
            {
              return new ResultWrapper<IList<Guid>>(new List<Guid>());
            }
        }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<IList<Guid>>(new List<Guid>(), e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<IList<Guid>>(new List<Guid>(), e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<bool> TrySubmitEntry(Guid cacheId, string messageType, string messageBody, int ttlSeconds = 86400) // default TTL: 1 day
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<bool>(false, new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<bool>(false, new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        string insertSQL = $"insert into DBCache.{tableName} (EntryId, SubmitDate, UpdateDate, MessageType, MessageBody, ExpirationDate) values (@EntryId, GETUTCDATE(), GETUTCDATE(), @MessageType, @MessageBody, @ExpirationDate)";
        using (SqlCommand insertCommand = new SqlCommand(insertSQL, refConnection))
        {
          insertCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
          insertCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
          insertCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)messageBody ?? DBNull.Value;
          insertCommand.Parameters.Add("@ExpirationDate", SqlDbType.DateTime2).Value = ttlSeconds > 0 ? (object)DateTime.UtcNow.AddSeconds(ttlSeconds) : DBNull.Value;
          insertCommand.ExecuteNonQuery();
          return new ResultWrapper<bool>(true);
        }
      }
      catch (SqlException e)
      {
        if (e.Number == 2627) // MSSQL_ENG002627 -- Violation of %ls constraint '%.*ls'. Cannot insert duplicate key in object '%.*ls'.
          return new ResultWrapper<bool>(false, e, FailureReason.AlreadyExists); // special handler for key duplicate
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<bool>(false, e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<bool>(false, e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<string> TrySubmitOrUpdateEntry(Guid cacheId, string messageType, string messageBody, int ttlSeconds = 86400) // default TTL: 1 day
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<string>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<string>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        using (SqlTransaction transaction = refConnection.BeginTransaction())
          try
          {
            string oldData = null;
            string selectSQL = $"select MessageBody, ExpirationDate from DBCache.{tableName}  WITH (XLOCK, HOLDLOCK) where EntryId = @EntryId and MessageType = @MessageType"; // must return 1 row, because EntryId + MessageType = primary key
            using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection, transaction))
            {
              selectCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
              selectCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
              using (SqlDataReader reader = selectCommand.ExecuteReader())
                if (reader.HasRows && reader.Read())
                {
                  // not expired
                  if (reader.IsDBNull(1) || (!reader.IsDBNull(1) && reader.GetDateTime(1) > DateTime.UtcNow))
                    oldData = reader.GetString(0);
                  else
                    oldData = null;
                  // update
                  reader.Close();
                  string updateSQL = $"update DBCache.{tableName} set UpdateDate = GETUTCDATE(), MessageBody = @MessageBody, ExpirationDate = @ExpirationDate where EntryId = @EntryId and MessageType = @MessageType";
                  using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection, transaction))
                  {
                    updateCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
                    updateCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
                    updateCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)messageBody ?? DBNull.Value;
                    updateCommand.Parameters.Add("@ExpirationDate", SqlDbType.DateTime2).Value = ttlSeconds > 0 ? (object)DateTime.UtcNow.AddSeconds(ttlSeconds) : DBNull.Value;
                    updateCommand.ExecuteNonQuery();
                  }
                }
                else
                {
                  // insert
                  reader.Close();
                  oldData = null;
                  string insertSQL = $"insert into DBCache.{tableName} (EntryId, SubmitDate, UpdateDate, MessageType, MessageBody, ExpirationDate) values (@EntryId, GETUTCDATE(), GETUTCDATE(), @MessageType, @MessageBody, @ExpirationDate)";
                  using (SqlCommand insertCommand = new SqlCommand(insertSQL, refConnection, transaction))
                  {
                    insertCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
                    insertCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
                    insertCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)messageBody ?? DBNull.Value;
                    insertCommand.Parameters.Add("@ExpirationDate", SqlDbType.DateTime2).Value = ttlSeconds > 0 ? (object)DateTime.UtcNow.AddSeconds(ttlSeconds) : DBNull.Value;
                    insertCommand.ExecuteNonQuery();
                  }
                }
              return new ResultWrapper<string>(oldData);
            }
          }
          catch (Exception e)
          {
            transaction.Rollback();
            throw e; // this catch is for transaction only
          }
          finally
          {
            transaction.Commit();
          }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<string> TryUpdateEntry(Guid cacheId, string messageType, CacheEntryUpdateCallback messageUpdater, int ttlSeconds = 86400) // default TTL: 1 day
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<string>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<string>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        using (SqlTransaction transaction = refConnection.BeginTransaction())
          try
          {
            string oldData = null;
            string selectSQL = $"select MessageBody, ExpirationDate from DBCache.{tableName}  WITH (XLOCK, HOLDLOCK) where EntryId = @EntryId and MessageType = @MessageType"; // must return 1 row, because EntryId + MessageType = primary key
            using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection, transaction))
            {
              selectCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
              selectCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
              using (SqlDataReader reader = selectCommand.ExecuteReader())
                if (reader.HasRows && reader.Read())
                {
                  // not expired
                  if (reader.IsDBNull(1) || (!reader.IsDBNull(1) && reader.GetDateTime(1) > DateTime.UtcNow))
                    oldData = reader.GetString(0);
                  else
                    oldData = null;
                  // update
                  reader.Close();
                  string updateSQL = $"update DBCache.{tableName} set UpdateDate = GETUTCDATE(), MessageBody = @MessageBody, ExpirationDate = @ExpirationDate where EntryId = @EntryId and MessageType = @MessageType";
                  using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection, transaction))
                  {
                    updateCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
                    updateCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
                    updateCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)messageUpdater(oldData) ?? DBNull.Value;
                    updateCommand.Parameters.Add("@ExpirationDate", SqlDbType.DateTime2).Value = ttlSeconds > 0 ? (object)DateTime.UtcNow.AddSeconds(ttlSeconds) : DBNull.Value;
                    updateCommand.ExecuteNonQuery();
                  }
                }
                else
                  return new ResultWrapper<string>(new KeyNotFoundException("Cache entity doesn't exist"), FailureReason.NotFound);
              return new ResultWrapper<string>(oldData);
            }
          }
          catch (Exception e)
          {
            transaction.Rollback();
            throw e; // this catch is for transaction only
          }
          finally
          {
            transaction.Commit();
          }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<string> TryUpdateEntry(Guid cacheId, string messageType, string messageBody, int ttlSeconds = 86400) // default TTL: 1 day
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<string>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<string>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        using (SqlTransaction transaction = refConnection.BeginTransaction())
          try
          {
            string oldData = null;
            string selectSQL = $"select MessageBody, ExpirationDate from DBCache.{tableName}  WITH (XLOCK, HOLDLOCK) where EntryId = @EntryId and MessageType = @MessageType"; // must return 1 row, because EntryId + MessageType = primary key
            using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection, transaction))
            {
              selectCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
              selectCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
              using (SqlDataReader reader = selectCommand.ExecuteReader())
                if (reader.HasRows && reader.Read())
                {
                  // not expired
                  if (reader.IsDBNull(1) || (!reader.IsDBNull(1) && reader.GetDateTime(1) > DateTime.UtcNow))
                    oldData = reader.GetString(0);
                  else
                    oldData = null;
                  // update
                  reader.Close();
                  string updateSQL = $"update DBCache.{tableName} set UpdateDate = GETUTCDATE(), MessageBody = @MessageBody, ExpirationDate = @ExpirationDate where EntryId = @EntryId and MessageType = @MessageType";
                  using (SqlCommand updateCommand = new SqlCommand(updateSQL, refConnection, transaction))
                  {
                    updateCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
                    updateCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
                    updateCommand.Parameters.Add("@MessageBody", SqlDbType.NVarChar).Value = (object)messageBody ?? DBNull.Value;
                    updateCommand.Parameters.Add("@ExpirationDate", SqlDbType.DateTime2).Value = ttlSeconds > 0 ? (object)DateTime.UtcNow.AddSeconds(ttlSeconds) : DBNull.Value;
                    updateCommand.ExecuteNonQuery();
                  }
                }
                else
                  return new ResultWrapper<string>(new KeyNotFoundException("Cache entity doesn't exist"), FailureReason.NotFound);
              return new ResultWrapper<string>(oldData);
            }
          }
          catch (Exception e)
          {
            transaction.Rollback();
            throw e; // this catch is for transaction only
          }
          finally
          {
            transaction.Commit();
          }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<string> TryPeekEntry(Guid cacheId, string messageType)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<string>(new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<string>(new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        string selectSQL = $"select MessageBody, ExpirationDate from DBCache.{tableName} where EntryId = @EntryId and MessageType = @MessageType"; // must return 1 row, because EntryId + MessageType = primary key
        using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection))
        {
          selectCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
          selectCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
          using (SqlDataReader reader = selectCommand.ExecuteReader())
            if (reader.HasRows && reader.Read())
            {
              // not expired
              if (reader.IsDBNull(1) || (!reader.IsDBNull(1) && reader.GetDateTime(1) > DateTime.UtcNow))
                return new ResultWrapper<string>(reader.GetString(0));
              else
                return new ResultWrapper<string>(new Exception("Requested entry has expired."), FailureReason.Constraint);
            }
          return new ResultWrapper<string>(new KeyNotFoundException("Requested entry not found"), FailureReason.NotFound);
        }
      }
      catch (SqlException e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.ApplicationError);
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<string>(e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public ResultWrapper<bool> TryRemoveEntry(Guid cacheId, string messageType)
    {
      try
      {
        if (string.IsNullOrWhiteSpace(messageType))
          return new ResultWrapper<bool>(false, new ArgumentNullException("messageType", "messageType argument is required."), FailureReason.ApplicationError);
        if (messageType.Length > 250)
          return new ResultWrapper<bool>(false, new ArgumentOutOfRangeException("messageType", "messageType argument must not exceed 250 characters."), FailureReason.ApplicationError);

        string deleteSQL = $"delete from DBCache.{tableName} where EntryId = @EntryId and MessageType = @MessageType"; // must delete 1 row, because EntryId + MessageType = primary key
        using (SqlCommand deleteCommand = new SqlCommand(deleteSQL, refConnection))
        {
          deleteCommand.Parameters.Add("@EntryId", SqlDbType.UniqueIdentifier).Value = cacheId;
          deleteCommand.Parameters.Add("@MessageType", SqlDbType.NVarChar, 250).Value = messageType;
          deleteCommand.ExecuteNonQuery();
          return new ResultWrapper<bool>(true);
        }
      }
      catch (Exception e)
      {
        refConnection.Close();
        refConnection.Open();
        return new ResultWrapper<bool>(false, e, FailureReason.SystemError);
      }
      finally
      {
        try
        {
          RemoveExpiredEntry();
        }
        catch
        {
          // do nothing
        }
      }
    }

    public IList<Tuple<Guid, string>> GetCacheEntries()
    {
      List<Tuple<Guid, string>> Result = new List<Tuple<Guid, string>>(100);
      string selectSQL = $"select EntryId, MessageType from DBCache.{tableName}";
      using (SqlCommand selectCommand = new SqlCommand(selectSQL, refConnection))
      {
        using (SqlDataReader reader = selectCommand.ExecuteReader())
          if (reader.HasRows)
            while (reader.Read())
            {
              Result.Add(new Tuple<Guid, string>(reader.GetGuid(0), reader.GetString(1)));
            }
      }
      return Result;
    }

    private void RemoveExpiredEntry()
    {
      try
      {
        string cleanupSQL = $"delete from DBCache.{tableName} where ExpirationDate < GETUTCDATE()";
        using (SqlCommand cleanupCommand = new SqlCommand(cleanupSQL, refConnection))
          cleanupCommand.ExecuteNonQuery();
      }
      catch
      {
        refConnection.Close();
        refConnection.Open();
      }
    }

    private void PrepareTable()
    {
      RunSQLCommand($"IF not exists (select * from sys.schemas WHERE name = 'DBCache') BEGIN EXEC('CREATE SCHEMA [DBCache]') END", refConnection);
      RunSQLCommand(@"IF not exists (select * from sys.tables t join sys.schemas s on s.schema_id=t.schema_id where t.name = '" + tableName + @"' and s.name = 'DBCache')
BEGIN
  CREATE TABLE DBCache." + tableName + @"
  (
    EntryId uniqueidentifier not null,
    SubmitDate DATETIME2,
    UpdateDate DATETIME2,
    MessageType NVARCHAR(250) not null,
    MessageBody NVARCHAR(MAX),
    ExpirationDate DATETIME2,
    constraint PK_" + tableName + @"MessageTypeID primary key (EntryId, MessageType)
  )
END", refConnection);
    }

    protected static void RunSQLCommand(string command, SqlConnection connection)
    {
      using (SqlCommand sqlCommand = new SqlCommand(command, connection))
      {
        sqlCommand.ExecuteNonQuery();
      }
    }

    public void Dispose()
    {
      refConnection.Dispose();
    }
  }

  public class MemoryBackedDBCache
  {
    private DBCache dbCache;
    private Dictionary<Guid, string> memoryCache, outputQueue;
    private List<Guid> removalQueue;
    private string messageType;

    public MemoryBackedDBCache(SqlConnection sqlConnection, string tableName, string messageType)
    {
      DefaultConstructor(sqlConnection, tableName, messageType);
    }

    public MemoryBackedDBCache(string databaseConnectionString, string tableName, string messageType)
    {
      DefaultConstructor(new SqlConnection(databaseConnectionString), tableName, messageType);
    }

    protected void DefaultConstructor(SqlConnection sqlConnection, string tableName, string entryType)
    {
      messageType = entryType;
      dbCache = new DBCache(sqlConnection, tableName);
      memoryCache = new Dictionary<Guid, string>();
      outputQueue = new Dictionary<Guid, string>();
      removalQueue = new List<Guid>();
    }

    public string ReadEntry(Guid entryId)
    {
      TryFlushOutputQueues(); // use opportunity to flush queues
      if (memoryCache.ContainsKey(entryId))
        if (removalQueue.Contains(entryId)) // just a precaution, the second contition should always be true
          throw new KeyNotFoundException("Unknown cache entry Id.");
        else
          return memoryCache[entryId];
      else
      {
        ResultWrapper<string> readResult = dbCache.TryPeekEntry(entryId, messageType);
        if (readResult.IsOK)
        {
          memoryCache.Add(entryId, readResult.Result);
          return readResult.Result;
        }
        else
        {
          if (readResult.FailureReason == FailureReason.NotFound)
            throw new KeyNotFoundException("Unknown cache entry Id.");
          else
            throw new Exception("Failed to read cache entry from DB.", readResult.Exception ?? new Exception("Unknown inner exception, failure reason: " + readResult.FailureReason.ToString()));
        }
      }
    }

    public void RemoveEntry(Guid entryId)
    {
      if (outputQueue.ContainsKey(entryId))
        outputQueue.Remove(entryId); // remove from output queue if hasn't flushed yet
      if (memoryCache.ContainsKey(entryId))
        memoryCache.Remove(entryId); // remove from memory cache
      if (!removalQueue.Contains(entryId))
        removalQueue.Add(entryId); // add to removal queue if hasn't added yet
      TryFlushOutputQueues(); // flush all queues
    }

    public void WriteEntry(Guid entryId, string properties)
    {
      if (outputQueue.ContainsKey(entryId))
        outputQueue[entryId] = properties;
      else
        outputQueue.Add(entryId, properties);
      if (removalQueue.Contains(entryId))
        removalQueue.Remove(entryId); // "undelete" if writes

      TryFlushOutputQueues();

      if (memoryCache.ContainsKey(entryId))
        memoryCache[entryId] = properties;
      else
        memoryCache.Add(entryId, properties);
    }

    protected void TryFlushOutputQueues()
    {
      if (outputQueue != null && outputQueue.Count > 0)
      {
        List<Guid> successWrites = new List<Guid>(outputQueue.Count);
        foreach (KeyValuePair<Guid, string> outstandingElement in outputQueue)
        {
          ResultWrapper<string> writeResult = dbCache.TrySubmitOrUpdateEntry(outstandingElement.Key, messageType, outstandingElement.Value, -1);
          if (writeResult.IsOK)
            successWrites.Add(outstandingElement.Key);
          else
            LastDBCacheWriteError = writeResult.Exception;
        }
        foreach (Guid writtenId in successWrites)
          outputQueue.Remove(writtenId);
      }
      if (removalQueue != null && removalQueue.Count > 0)
      {
        List<Guid> successRemovals = new List<Guid>();
        foreach (Guid outstandingElement in removalQueue)
        {
          ResultWrapper<bool> deleteResult = dbCache.TryRemoveEntry(outstandingElement, messageType);
          if (deleteResult.IsOK && deleteResult.Result)
            successRemovals.Add(outstandingElement);
          else
            LastDBCacheWriteError = deleteResult.Exception;
        }
        foreach (Guid removedId in successRemovals)
          removalQueue.Remove(removedId);
      }
    }

    #region Diagnostics
    public int OutstandingDBCacheQueueLength => outputQueue.Count;
    public Exception LastDBCacheWriteError { get; protected set; }
    #endregion
  }
}