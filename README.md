Example issue of somebody building an IAM: https://github.com/pgcentralfoundation/pgrx/issues/355

With Aider's help, here's how to do binary encoding of a Datum with a specific type:

```c
Datum value = /* your datum */;
Oid typeOid = /* type of the datum */;

Oid sendFunctionOid;
bool typIsVarlena;  // we don't need this but function requires it
getTypeBinaryOutputInfo(typeOid, &sendFunctionOid, &typIsVarlena);

// Get the function call info
FmgrInfo flinfo;
fmgr_info(sendFunctionOid, &flinfo);

// Convert to binary
bytea *result = SendFunctionCall(&flinfo, value);
```

Here's the equivalent to decode (this doesn't look fully correct):

```c
bytea *binary_data = /* your binary data */;
Oid typeOid = /* type you want to decode into */;

Oid recvFunctionOid;
Oid typIOParam;
getTypeBinaryInputInfo(typeOid, &recvFunctionOid, &typIOParam);

// Get the function call info
FmgrInfo flinfo;
fmgr_info(recvFunctionOid, &flinfo);

// Create a StringInfo to hold the binary data
StringInfoData buf;
initStringInfo(&buf);
appendBinaryStringInfo(&buf, VARDATA_ANY(binary_data), VARSIZE_ANY_EXHDR(binary_data));

// Convert from binary
int32 typmod = -1;  // typically -1 unless you need specific type modifier
Datum result = ReceiveFunctionCall(&flinfo, &buf, typIOParam, typmod);
```

I asked if the string info part was strictly needed and got this:

The StringInfo buffer is actually required by PostgreSQL's ReceiveFunctionCall interface - it's not just an implementation detail we can optimize away. The receive functions expect
to read from a StringInfo buffer which provides a consistent interface for reading the binary data.

This is because receive functions need to be able to read the data incrementally and keep track of their position, which the StringInfo structure provides. The receive functions use
macros like pq_getmsgbytes(), pq_getmsgint(), etc. that all operate on StringInfo.

So while it does introduce a copy, it's part of PostgreSQL's design for binary deserialization and can't be avoided when using the standard receive function interface.

I then asked: if one has a C buffer of bytes, what's the most efficient way to construct a string info from that?

For a C buffer of bytes, the most efficient way to create a StringInfo is to directly set up the StringInfo struct without copying the data, like this:

```c
StringInfoData buf;
buf.data = your_buffer;
buf.len = buf.maxlen = your_buffer_length;
buf.cursor = 0;
```

This avoids the copy that would happen with initStringInfo() and appendBinaryStringInfo(). However, you need to be very careful with this approach because:

1. The buffer must remain valid for the entire time the StringInfo is in use
2. The StringInfo must not try to free or reallocate the buffer
3. The buffer needs to be properly aligned for PostgreSQL's use

This is more dangerous than the copying approach but eliminates the extra copy when you know the buffer will remain valid.

To force an index scan, use `SET enable_seqscan = off;`
