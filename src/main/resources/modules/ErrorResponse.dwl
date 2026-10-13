%dw 2.0
fun build(code, message, corrId, fields = []) = {
  errorCode: code,
  message: message,
  correlationId: corrId,
  (fields: fields) if (!isEmpty(fields))
}

/* Only JSON-pointer paths are echoed back (never the offending VALUE, which could be a card number). */
fun fieldPaths(desc) = ((desc default "") scan /#\/[A-Za-z0-9_\/\[\]]+/) map ($[0]) distinctBy $
