# simdcsv

simdcsv is a CSV parser that evaluates 64 bytes at a time. There are many kinds of CSV files; this project adheres to the format described
in [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180.html).

**Introduction**

We can classify every character in CSV into the following: a COMMA, QUOTATION, NEW_LINE, OTHER. We can build a perfect lookup table and use `vqtbl1q_u8` to classify 16 characters at once. Daniel Lemire calls this "vectorized classification" in the simdjson paper. [[code pointer]](https://github.com/friendlymatthew/simdcsv/blob/main/src/classifier.rs)

Once we classify every character, we can build a bitset for each class. We chunk through 64 characters at a time, building a `u64` for every chunk. Here is a naive case:

```
[//]: # COMMA = 0, QUOTATION = 1, NEW_LINE = 2, OTHER = 3

aaa,bbb,ccc
33303330333
```

Then the bitsets look like:

```rs
comma_bitset = 0b00010001000
other_bitset = 0b11101110111
```

Now, we can just [count the number of leading zeros](https://doc.rust-lang.org/std/primitive.u64.html#method.leading_zeros) in the comma bitset to pull the csv entries.

Using a bitset is pretty powerful in cases where one wants to check if there exists a symbol, count the # of symbols, or remove escaped symbols.

**Detecting Escaped Quotations and Commas**

Consider the csv row: `"aaa,norm","b""bb","ccc"`

In CSV, quotes are escaped by doubling them (`""`). The `""` in `b""bb` is field content, not a structural delimiter. We detect escaped pairs by finding adjacent quotes:

```rs
let escaped = q & (q << 1);        // Find adjacent quote pairs
let escaped = escaped | (escaped >> 1);  // Mark both quotes in each pair
let valid_quotes = q & !escaped;   // Remove escaped quotes
```

```rs
quote_bitset   = 0b100010010011010000000001
q << 1         = 0b000100100110100000000010
escaped        = 0b000000010000000000000000  // Found the "" pair
escaped | >> 1 = 0b000000011000000000000000  // Both quotes marked
valid_quotes   = 0b100010000011010000000001  // Only structural quotes remain
```

**Marking Inside Quotations**

With only structural quotes, we use parallel prefix XOR to mark all bits between quote pairs:

```rs
valid_quotes  = 0b100010000011010000000001  // Structural quotes only
inside_quotes = 0b011100011111000011111110  // All bits between quote pairs marked as 1
```

Masking out commas inside quotes:

```rs
comma_bitset  = 0b000001000000100000010000  // Commas at positions 4, 10, 18
valid_commas  = comma_bitset & !inside_quotes
              = 0b000001000000100000000000  // Comma at 4 masked out
```

## Reading

https://www.rfc-editor.org/rfc/rfc4180.html<br>
https://arxiv.org/pdf/1902.08318<br>
https://branchfree.org/2019/03/06/code-fragment-finding-quote-pairs-with-carry-less-multiply-pclmulqdq/<br>
