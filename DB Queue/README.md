# Database Persistent Objects #

Within micros-service application architecture, different services should be able to send eachother messages for processing and save some persistent data for other components. There is multiple message queue soulutions at the market, however, I needed a portable one, which doesn't require any software installation or specific participating host configuration.
Like most of other solutions this implementation uses a database for queue/cache storage, and some sophisticated locks to allow concurent read from multiple instances.

## Database Queue ##
`DBQueue` class prides message queue functionality. All methods follow the "try" pattern, i.e. they never trows an exception, but instead return either result or failure status using Result Wrapper class.

### Constructors ###
```
public DBQueue(SqlConnection sqlConnection, string tableName)
```
Creates a new instance of database queue using the given SQL Server connection and the specified table. If the table doesn't exist, it will be created in the 'DBQueue' schema.

### Methods ###
```
public ResultWrapper<long> TrySubmitMessage(string messageType, string message, DateTime? deliveryAfterUTC = null)
```
Submits a new message of the given message type, and returns the message ID. By default, the submitted message is immideately available for pick up. The optional parameter `deliveryAfterUTC` allows pospone delivery until given time.

```
public ResultWrapper<Tuple<long, string>> TryPeekMessageForProcessing(string messageType, int messageTimeoutSeconds)
```
Picks a message of specific type for processing. The `messageTimeoutSeconds` parameter sets interval when the message will be returned back to the queue, unless the message is explicitly marked completed or failed. (see `TrySetMessageFailed` and `TrySetMessageCompleted` methods). The returned value contains message ID and message body inside a Tuple object.

```
public ResultWrapper<bool> TrySetMessageTimeout(ulong MessageId, int newTimeoutSeconds)
```
A process can extend processing timeout to prevent the message to be returned back to the queue. The process shouldn't call this method if it hasn't picked the message for processing. No SQL locks are used.

```
public ResultWrapper<bool> TrySetMessageFailed(long MessageId, string messageStatusMessage)
```
Set a message as completed with failure. The 'messageStatusMessage' parameter may contain failure reason. When message is completed, it's automatically deleted from the main table and moved to archive table.

```
public ResultWrapper<bool> TrySetMessageCompleted(long MessageId, string messageStatusMessage)
```
Set a message as completed with success. The 'messageStatusMessage' parameter may contain completition notes. When message is completed, it's automatically deleted from the main table and moved to archive table.

### Advanced Methods ###
```
public ResultWrapper<Tuple<long, string>> TryPeekMessageForParallelProcessing(string messageType, string instanceId, int messageTimeoutSeconds)
```
This method allows process a message in an unlimited number of parallel processes. Unlike `TryPeekMessageForProcessing` method, `TryPeekMessageForParallelProcessing` doesn't remove the message from available queue, so it can be picked by another parallel process (however, it cannot be picked for exclusive processing with the `TryPeekMessageForProcessing` method). The `instanceId` parameter contains uniqueu parallel process ID, so the same message cannot be processed by parallel processes with the same ID. In other words, this method maintein per-instance queue for each message selected for parallel processing. The `messageTimeoutSeconds` parameter sets interval when the message will be returned back to the per-instance queue.
*Note*: when a root message is picked for parallel processing, its timeout is set to not expire. Use non-parallel `TrySetMessageFailed` and `TrySetMessageCompleted` methods to complete the root message.

```
public ResultWrapper<bool> TrySetMessageParallelFailed(long MessageId, string instanceId)
```
Set a message as completed with failure for the specific parallel instance. The 'messageStatusMessage' parameter may contain failure reason. This completition status doesn't affect the root message status, which should be set explicitly by calling non-parallel `TrySetMessageFailed` and `TrySetMessageCompleted` methods. There is no way to rollup indicidual parallel statuses to the root message status. This is because the number of parallel processes is unknown, and a new parallel process can be added at any time until the root message hasn't completed.

```
public ResultWrapper<bool> TrySetMessageParallelCompleted(long MessageId, string instanceId)
```
Set a message as completed with success for the specific parallel instance. The 'messageStatusMessage' parameter may contain success notes. This completition status doesn't affect the root message status, which should be set explicitly by calling either non-parallel `TrySetMessageFailed` or `TrySetMessageCompleted` methods. There is no way to rollup indicidual parallel statuses to the root message status, because the number of parallel processes is unknown, and a new parallel process can be added at any time until the root message hasn't completed.

## Database Cache ##
`DBCache` class implements a persistent cache to transfer information between micro services.
