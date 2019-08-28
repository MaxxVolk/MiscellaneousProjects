# Result Wrapper #

Result wrapper is a wrapper object to return a result among with operation execution status. It's an alternative to Win32 API style when a function/method returns error code or success, but actual result is writen into reffered variable. This class combine them both in one obbject.

It's not my original idea, but just yet another implementation.

The namespace provides `FailureReason` enumeration with some most common failure reasons and generic class `public class ResultWrapper<ReturnType>`. There is a number of constructors accepting combinations of returned result, failure reason, and an exception.

## Example ##
### Method Declaration ###
`public ResultWrapper<Tuple<long, string>> TryPeekMessageForProcessing(string messageType, int messageTimeoutSeconds)
{
  <...>
  return new ResultWrapper<Tuple<long, string>>(result);
  <...>
  return new ResultWrapper<Tuple<long, string>>(FailureReason.NotFound);
}`

### Usage ###
'
ResultWrapper<Tuple<long, string>> currentTask = <...>.TryPeekMessageForProcessing(DBNames.EnrollCertificateTask, 180);
if (currentTask.IsOK)
{
  <...>
}
'
