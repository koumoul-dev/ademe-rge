// The ADEME CSV files are encoded in windows-1252 (ANSI).
// Node's built-in TextDecoder handles this encoding, no native addon needed.
const decoder = new TextDecoder('windows-1252')

// these 5 bytes have no meaning in windows-1252 ; TextDecoder silently maps them
// to C1 control characters, whereas libiconv used to fail with EILSEQ.
// We keep the EILSEQ behavior so that the validation can report a probable encoding error.
const undefinedBytes = new Set([0x81, 0x8d, 0x8f, 0x90, 0x9d])

exports.decodeCp1252 = (buf) => {
  for (const b of buf) {
    if (undefinedBytes.has(b)) {
      const err = new Error('Illegal character sequence.')
      err.code = 'EILSEQ'
      throw err
    }
  }
  return decoder.decode(buf)
}
