# Database Persistent Objects #

Within micros-service application architecture, different services should be able to send eachother messages for processing and save some persistent data for other components. There is multiple message queue soulutions at the market, however, I needed a portable one, which doesn't require any software installation or specific participating host configuration.
Like most of other solutions this implementation uses a database for queue/cache storage, and some sophisticated locks to allow concurent read from multiple instances.

## Database Queue ##
`DBQueue` class prides message queue functionality. All methods follow the "try" pattern, i.e. they never trows an exception, but instead return either result or failure status using Result Wrapper class.

### Constructors ###
`public DBQueue(SqlConnection sqlConnection, string tableName)`
Creates a new instance of database queue using the given SQL Server connection and the specified table. If the table doesn't exist, it will be created in the 'DBQueue' schema.

### Methods ###
`public ResultWrapper<long> TrySubmitMessage(string messageType, string message, DateTime? deliveryAfterUTC = null)`
Submits a new message of the given message type, and returns the message ID. By default, the submitted message is immideately available for pick up. The optional parameter `deliveryAfterUTC` allows pospone delivery until given time.

`public ResultWrapper<Tuple<long, string>> TryPeekMessageForProcessing(string messageType, int messageTimeoutSeconds)`
Picks a message of specific type for processing. The `messageTimeoutSeconds` parameter sets interval when the message will be returned back to the queue, unless the message is explicitly marked completed or failed. (see `TrySetMessageFailed` and `TrySetMessageCompleted` methods).
