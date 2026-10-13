%dw 2.0
/* Keep only the last four characters; everything else becomes X. */
fun maskCard(s) = do {
  var str = (s default "") as String
  var n = sizeOf(str)
  ---
  if (n <= 4) "XXXX" else (((1 to (n - 4)) map "X") joinBy "") ++ str[-4 to -1]
}

/* Strip anything that looks like a raw card number from free text (error messages, reasons). */
fun sanitize(s) = (s default "") as String replace /(\d[ -]?){13,19}/ with "[REDACTED]"
